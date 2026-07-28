// crates/qbzd/src/playback_events.rs: the playback-to-bus publisher (CONSOLE ext).
//
// The core never emits `CoreEvent::{TrackStarted, PlaybackStateChanged,
// PositionUpdated, VolumeChanged}`: on the desktop those four are read off the
// player by the 450 ms Slint poll loop (`qbz/src/playback.rs`) and pushed to the
// UI/tray/MPRIS by hand. The daemon has no poll loop, and its playback surfaces
// (`mpris.rs`, `scrobble_engine.rs`, `api/sse.rs`) all subscribe to the adapter
// bus, so they were listening to a channel that carried nothing.
//
// One publisher, not three pollers: mirror the desktop poll here and emit the
// edges onto the bus. Playback origin is irrelevant: CLI, HTTP API and a
// QConnect controller all drive the one `core().player()`.
//
// Like `mpris.rs`, the task holds only a `Weak<AppRuntime>` (upgraded per tick),
// so it never pins the runtime and the #521 audio-release ordering is unaffected.
use std::sync::{Arc, Weak};
use std::time::Duration;

use qbz_app::shell::AppRuntime;
use qbz_models::{CoreEvent, PlaybackState};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::adapter::DaemonAdapter;

type Runtime = Arc<AppRuntime<DaemonAdapter>>;

/// Poll cadence: the desktop's 450 ms (`playback.rs:4088`), matching the driver
/// so state edges surface within one driver tick. Pure reader: never commands.
const TICK_MS: u64 = 450;

/// Ticks to wait for the queue cursor to catch up with the id the engine is
/// actually playing before latching the change anyway. A gapless hand-off
/// leaves the two disagreeing until the driver's `SyncCursorTo` lands.
const CURSOR_SYNC_GRACE_TICKS: u32 = 8;

/// Previous tick's published view, so every emission is edge-triggered.
struct Last {
    track_id: u64,
    state: PlaybackState,
    position: u64,
    volume: f32,
    waiting: u32,
}

impl Default for Last {
    fn default() -> Self {
        // volume NaN, not 0.0: a real volume of 0 must still emit on tick one.
        Last { track_id: 0, state: PlaybackState::Stopped, position: 0, volume: f32::NAN, waiting: 0 }
    }
}

/// Spawn the publisher. Shutdown aborts+joins it, so no upgraded `Arc` can be in
/// flight when `drop(booted)` releases the audio device.
pub fn spawn(runtime: &Runtime, bus: broadcast::Sender<CoreEvent>) -> JoinHandle<()> {
    let weak: Weak<AppRuntime<DaemonAdapter>> = Arc::downgrade(runtime);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(TICK_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut last = Last::default();

        loop {
            ticker.tick().await;
            let Some(rt) = weak.upgrade() else { return };
            tick(&rt, &bus, &mut last).await;
            drop(rt); // Weak-only while parked on the next tick.
        }
    })
}

/// One poll: read the player, diff against `last`, publish what changed.
async fn tick(rt: &Runtime, bus: &broadcast::Sender<CoreEvent>, last: &mut Last) {
    let core = rt.core();
    let player = core.player();
    let ev = player.get_playback_event();
    let state = derive_state(ev.is_playing, player.has_loaded_audio());

    // Track change first, so subscribers see the metadata before the status that
    // refers to it (an MPRIS widget otherwise flashes the previous track).
    if ev.track_id != 0 && ev.track_id != last.track_id {
        let queue = core.get_queue_state().await;
        match queue.current_track {
            Some(track) if track.id == ev.track_id => {
                let _ = bus.send(CoreEvent::TrackStarted { track, position_secs: ev.position });
                last.track_id = ev.track_id;
                last.waiting = 0;
            }
            // Mid gapless hand-off: retry next tick rather than publish the wrong
            // track, but give up after the grace window so a stuck cursor cannot
            // make this a permanent per-tick queue read.
            _ => {
                last.waiting += 1;
                if last.waiting >= CURSOR_SYNC_GRACE_TICKS {
                    log::debug!(
                        "[playback-events] cursor never reached playing track {}, no TrackStarted",
                        ev.track_id
                    );
                    last.track_id = ev.track_id;
                    last.waiting = 0;
                }
            }
        }
    }

    if state != last.state {
        last.state = state;
        let _ = bus.send(CoreEvent::PlaybackStateChanged { state });
    }

    // Whole seconds, so ~1 event/s while playing and silent when paused.
    if state == PlaybackState::Playing && ev.position != last.position {
        last.position = ev.position;
        let _ = bus.send(CoreEvent::PositionUpdated {
            position_secs: ev.position,
            duration_secs: ev.duration,
        });
    }

    if last.volume.to_bits() != ev.volume.to_bits() {
        last.volume = ev.volume;
        let _ = bus.send(CoreEvent::VolumeChanged { volume: ev.volume });
    }
}

/// The player's two booleans → the model state. `Loading` is never derived: the
/// player reports `is_playing` from the moment a stream is engaged.
fn derive_state(is_playing: bool, has_audio: bool) -> PlaybackState {
    if is_playing {
        PlaybackState::Playing
    } else if has_audio {
        PlaybackState::Paused
    } else {
        PlaybackState::Stopped
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_state_maps_the_player_booleans() {
        assert_eq!(derive_state(true, true), PlaybackState::Playing);
        assert_eq!(derive_state(true, false), PlaybackState::Playing);
        assert_eq!(derive_state(false, true), PlaybackState::Paused);
        assert_eq!(derive_state(false, false), PlaybackState::Stopped);
    }

    #[test]
    fn seed_emits_on_a_first_tick_of_zero_volume_and_stopped() {
        let last = Last::default();
        assert_ne!(last.volume.to_bits(), 0.0f32.to_bits());
        assert_eq!(last.state, PlaybackState::Stopped);
    }
}
