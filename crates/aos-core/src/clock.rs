// Copyright 2018 Open Source Robotics Foundation, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// Timer transition arithmetic is translated from ROS 2 Jazzy rcl commit
// 18d96c9c2b98e616324ae9a2c7701dd0b0886ff0, rcl/src/rcl/timer.c:65-125,283-319.
// AOS uses explicit initialization instead of the upstream now==0 sentinel,
// and Rust RAII leases ensure at most one queued/running executor callback.
use std::{
    collections::VecDeque,
    str::FromStr,
    sync::{Arc, Condvar, Mutex, OnceLock, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{lifecycle::LifecycleLease, Error, Lifecycle, LifecycleWorkKind, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClockType {
    Steady,
    System,
    Ros,
}
impl FromStr for ClockType {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        match value {
            "steady" => Ok(Self::Steady),
            "system" => Ok(Self::System),
            "ros" => Ok(Self::Ros),
            _ => Err(Error::InvalidArgument(format!(
                "unknown clock type {value:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimePoint {
    pub nanoseconds: i64,
    pub clock_type: ClockType,
    pub initialized: bool,
    pub use_sim_time: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClockJump {
    pub sequence: u64,
    pub clock_type: ClockType,
    pub before: Option<i64>,
    pub after: Option<i64>,
    pub delta: Option<i64>,
    pub reason: String,
}

pub(crate) trait ShutdownResource: Send + Sync {
    fn shutdown(&self);
    fn prepare_lifecycle_activation(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Clock {
    inner: Arc<ClockInner>,
}
type SimTimeSetter = Arc<dyn Fn(bool) -> Result<()> + Send + Sync>;
struct ClockInner {
    kind: ClockType,
    state: Mutex<ClockState>,
    sim_time_setter: Mutex<Option<SimTimeSetter>>,
}
struct ClockState {
    closed: bool,
    use_sim_time: bool,
    sim_time: Option<i64>,
    sequence: u64,
    forward_updates: u64,
    jumps: VecDeque<ClockJump>,
    timers: Vec<Weak<TimerInner>>,
}

impl Clock {
    pub fn new(kind: ClockType) -> Self {
        Self {
            inner: Arc::new(ClockInner {
                kind,
                sim_time_setter: Mutex::new(None),
                state: Mutex::new(ClockState {
                    closed: false,
                    use_sim_time: false,
                    sim_time: None,
                    sequence: 0,
                    forward_updates: 0,
                    jumps: VecDeque::new(),
                    timers: Vec::new(),
                }),
            }),
        }
    }
    pub fn now(&self) -> Result<TimePoint> {
        let state = self.inner.state.lock().unwrap();
        self.inner.now_locked(&state)
    }
    pub fn set_use_sim_time(&self, enabled: bool) -> Result<()> {
        let setter = self.inner.sim_time_setter.lock().unwrap().clone();
        if let Some(setter) = setter {
            return setter(enabled);
        }
        self.set_use_sim_time_direct(enabled)
    }
    pub(crate) fn bind_sim_time_setter(&self, setter: SimTimeSetter) -> Result<()> {
        let mut slot = self.inner.sim_time_setter.lock().unwrap();
        if slot.is_some() {
            return Err(Error::InvalidArgument(
                "ROS clock already has a parameter owner".into(),
            ));
        }
        *slot = Some(setter);
        Ok(())
    }
    pub(crate) fn set_use_sim_time_direct(&self, enabled: bool) -> Result<()> {
        self.configure_sim_time(enabled).map(|_| ())
    }
    pub(crate) fn configure_sim_time(&self, enabled: bool) -> Result<TimePoint> {
        if self.inner.kind != ClockType::Ros {
            return Err(Error::Unsupported(
                "use_sim_time requires a ROS clock".into(),
            ));
        }
        let mut state = self.inner.state.lock().unwrap();
        let before = self.inner.now_locked(&state)?;
        if state.use_sim_time == enabled {
            return Ok(before);
        }
        state.use_sim_time = enabled;
        let after = match self.inner.now_locked(&state) {
            Ok(after) => after,
            Err(error) => {
                state.use_sim_time = before.use_sim_time;
                return Err(error);
            }
        };
        self.inner.jump_locked(
            &mut state,
            before,
            after,
            if enabled {
                "ros_time_activated"
            } else {
                "ros_time_deactivated"
            },
            true,
        );
        Ok(after)
    }
    /// Supply a ROS clock sample. A sample at zero is initialized. Updating an
    /// inactive ROS clock stores its override without changing its system time.
    pub fn update_ros_time(&self, nanoseconds: i64) -> Result<()> {
        if self.inner.kind != ClockType::Ros {
            return Err(Error::Unsupported(
                "clock samples require a ROS clock".into(),
            ));
        }
        if nanoseconds < 0 {
            return Err(Error::InvalidArgument(
                "ROS time must be non-negative".into(),
            ));
        }
        let mut state = self.inner.state.lock().unwrap();
        let before = self.inner.now_locked(&state)?;
        state.sim_time = Some(nanoseconds);
        if !state.use_sim_time {
            return Ok(());
        }
        let after = self.inner.now_locked(&state)?;
        if before.initialized && before.nanoseconds == after.nanoseconds {
            return Ok(());
        }
        if !before.initialized {
            self.inner
                .jump_locked(&mut state, before, after, "initialized", false);
        } else if nanoseconds < before.nanoseconds {
            self.inner
                .jump_locked(&mut state, before, after, "backward", false);
        } else {
            // A sample contains no seek flag or publisher cadence. Coalesce
            // consecutive positive advances without inventing a jump threshold.
            // Fixed Jazzy timer.c:113-118 only wakes ready timers here; it does
            // not advance their next-call timestamp while a callback is active.
            state.forward_updates = state.forward_updates.saturating_add(1);
            if let Some(previous) = state
                .jumps
                .back_mut()
                .filter(|event| event.reason == "forward")
            {
                previous.after = Some(after.nanoseconds);
                previous.delta = previous
                    .before
                    .and_then(|before| after.nanoseconds.checked_sub(before));
            } else {
                self.inner
                    .record_jump_locked(&mut state, before, after, "forward");
            }
            state.timers.retain(|timer| {
                if let Some(timer) = timer.upgrade() {
                    // Synchronize with wait_and_take's condition check and
                    // Condvar wait, otherwise a paused Rate can lose this wake.
                    let _state = timer.state.lock().unwrap();
                    timer.changed.notify_all();
                    true
                } else {
                    false
                }
            });
        }
        Ok(())
    }
    /// Drain up to the last 256 jump observations. Consecutive forward samples
    /// share one record spanning the first before to the latest after; other
    /// transitions split these ranges. Sequence numbers count records, while
    /// forward_update_count counts individual positive samples.
    pub fn take_jump_events(&self) -> Vec<ClockJump> {
        self.inner.state.lock().unwrap().jumps.drain(..).collect()
    }
    pub fn forward_update_count(&self) -> u64 {
        self.inner.state.lock().unwrap().forward_updates
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
    pub(crate) fn shutdown_resource(&self) -> Arc<dyn ShutdownResource> {
        self.inner.clone()
    }
}

impl ClockInner {
    fn now_locked(&self, state: &ClockState) -> Result<TimePoint> {
        if state.closed {
            return Err(Error::Closed);
        }
        let (nanoseconds, initialized) = match self.kind {
            ClockType::Ros if state.use_sim_time => {
                (state.sim_time.unwrap_or(0), state.sim_time.is_some())
            }
            ClockType::System | ClockType::Ros => {
                let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
                    Error::InvalidArgument("system clock precedes Unix epoch".into())
                })?;
                (duration_ns(elapsed)?, true)
            }
            ClockType::Steady => {
                static EPOCH: OnceLock<Instant> = OnceLock::new();
                (
                    duration_ns(EPOCH.get_or_init(Instant::now).elapsed())?,
                    true,
                )
            }
        };
        Ok(TimePoint {
            nanoseconds,
            initialized,
            clock_type: self.kind,
            use_sim_time: state.use_sim_time,
        })
    }
    fn jump_locked(
        &self,
        state: &mut ClockState,
        before: TimePoint,
        after: TimePoint,
        reason: &str,
        source_change: bool,
    ) {
        self.record_jump_locked(state, before, after, reason);
        let before_ns = before.initialized.then_some(before.nanoseconds);
        let after_ns = after.initialized.then_some(after.nanoseconds);
        state.timers.retain(|timer| {
            if let Some(timer) = timer.upgrade() {
                timer.on_jump(before_ns, after_ns, source_change);
                true
            } else {
                false
            }
        });
    }
    fn record_jump_locked(
        &self,
        state: &mut ClockState,
        before: TimePoint,
        after: TimePoint,
        reason: &str,
    ) {
        state.sequence = state.sequence.saturating_add(1);
        let before_ns = before.initialized.then_some(before.nanoseconds);
        let after_ns = after.initialized.then_some(after.nanoseconds);
        if state.jumps.len() == 256 {
            state.jumps.pop_front();
        }
        state.jumps.push_back(ClockJump {
            sequence: state.sequence,
            clock_type: self.kind,
            before: before_ns,
            after: after_ns,
            delta: before_ns.zip(after_ns).and_then(|(b, a)| a.checked_sub(b)),
            reason: reason.into(),
        });
    }
    fn close(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        state.closed = true;
        for timer in state.timers.drain(..).filter_map(|timer| timer.upgrade()) {
            let _ = timer.close();
        }
        Ok(())
    }
}
impl ShutdownResource for ClockInner {
    fn shutdown(&self) {
        let _ = self.close();
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TimerInfo {
    pub expected_call_time: i64,
    pub actual_call_time: i64,
}

#[derive(Clone)]
pub struct Timer {
    inner: Arc<TimerInner>,
}
struct TimerInner {
    clock: Clock,
    period: i64,
    state: Mutex<TimerState>,
    changed: Condvar,
    managed: Option<ManagedTimer>,
}
struct ManagedTimer {
    lifecycle: Lifecycle,
    name: String,
    id: String,
}
struct TimerState {
    closed: bool,
    canceled: bool,
    in_flight: bool,
    generation: u64,
    last: Option<i64>,
    next: Option<i64>,
    credit: Option<i64>,
}

/// One queued or executing callback. Dropping or finishing it releases the
/// timer even after callback failure. Executors check is_valid just before
/// entering user code; cancellation cannot stop code that has already begun.
pub struct TimerCall {
    timer: Arc<TimerInner>,
    generation: u64,
    info: TimerInfo,
    _lifecycle_lease: Option<LifecycleLease>,
}
impl TimerCall {
    pub fn info(&self) -> TimerInfo {
        self.info
    }
    pub fn is_valid(&self) -> bool {
        let state = self.timer.state.lock().unwrap();
        !state.closed && !state.canceled && state.generation == self.generation
    }
    pub fn finish(self) {}
}
impl Drop for TimerCall {
    fn drop(&mut self) {
        self.timer.state.lock().unwrap().in_flight = false;
        self.timer.changed.notify_all();
    }
}
impl Timer {
    pub fn new(clock: Clock, period: Duration, autostart: bool) -> Result<Self> {
        Self::build(clock, period, autostart, None)
    }
    pub(crate) fn new_managed(
        clock: Clock,
        period: Duration,
        autostart: bool,
        lifecycle: Lifecycle,
        node_name: &str,
    ) -> Result<Self> {
        Self::build(
            clock,
            period,
            autostart,
            Some(ManagedTimer {
                lifecycle,
                name: format!("{node_name}/timer"),
                id: uuid::Uuid::new_v4().simple().to_string(),
            }),
        )
    }
    fn build(
        clock: Clock,
        period: Duration,
        autostart: bool,
        managed: Option<ManagedTimer>,
    ) -> Result<Self> {
        let period = duration_ns(period)?;
        let mut clock_state = clock.inner.state.lock().unwrap();
        let now = clock.inner.now_locked(&clock_state)?;
        let last = now.initialized.then_some(now.nanoseconds);
        let next = last
            .map(|now| now.checked_add(period).ok_or_else(time_overflow))
            .transpose()?;
        let inner = Arc::new(TimerInner {
            clock: clock.clone(),
            period,
            state: Mutex::new(TimerState {
                closed: false,
                canceled: !autostart,
                in_flight: false,
                generation: 0,
                last,
                next,
                credit: None,
            }),
            changed: Condvar::new(),
            managed,
        });
        clock_state.timers.retain(|timer| timer.strong_count() != 0);
        clock_state.timers.push(Arc::downgrade(&inner));
        Ok(Self { inner })
    }
    pub fn clock(&self) -> Clock {
        self.inner.clock.clone()
    }
    pub fn period(&self) -> Duration {
        Duration::from_nanos(self.inner.period as u64)
    }
    pub fn is_canceled(&self) -> bool {
        self.inner.state.lock().unwrap().canceled
    }
    pub fn cancel(&self) -> Result<()> {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        state.canceled = true;
        state.generation = state.generation.wrapping_add(1);
        self.inner.changed.notify_all();
        Ok(())
    }
    pub fn reset(&self) -> Result<()> {
        let clock_state = self.inner.clock.inner.state.lock().unwrap();
        let now = self.inner.clock.inner.now_locked(&clock_state)?;
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        state.last = now.initialized.then_some(now.nanoseconds);
        state.next = state
            .last
            .map(|now| now.checked_add(self.inner.period).ok_or_else(time_overflow))
            .transpose()?;
        state.credit = None;
        state.canceled = false;
        state.generation = state.generation.wrapping_add(1);
        self.inner.changed.notify_all();
        Ok(())
    }
    pub fn is_ready(&self) -> Result<bool> {
        if !self.inner.business_active()? {
            return Ok(false);
        }
        let clock_state = self.inner.clock.inner.state.lock().unwrap();
        let now = self.inner.clock.inner.now_locked(&clock_state)?;
        let state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        Ok(!state.canceled
            && !state.in_flight
            && now.initialized
            && state.next.is_some_and(|next| next <= now.nanoseconds))
    }
    pub fn time_until_next_call(&self) -> Result<Option<Duration>> {
        if !self.inner.business_active()? {
            return Ok(None);
        }
        let clock_state = self.inner.clock.inner.state.lock().unwrap();
        let now = self.inner.clock.inner.now_locked(&clock_state)?;
        let state = self.inner.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        if state.canceled || !now.initialized {
            return Ok(None);
        }
        Ok(state
            .next
            .map(|next| Duration::from_nanos(next.saturating_sub(now.nanoseconds).max(0) as u64)))
    }
    pub fn take_ready(&self) -> Result<Option<TimerCall>> {
        let clock_state = self.inner.clock.inner.state.lock().unwrap();
        let now = self.inner.clock.inner.now_locked(&clock_state)?;
        let mut state = self.inner.state.lock().unwrap();
        self.take_locked(&mut state, now)
    }
    fn take_locked(&self, state: &mut TimerState, now: TimePoint) -> Result<Option<TimerCall>> {
        if state.closed {
            return Err(Error::Closed);
        }
        if state.canceled || state.in_flight || !now.initialized {
            return Ok(None);
        }
        let Some(expected) = state.next else {
            return Ok(None);
        };
        if expected > now.nanoseconds {
            return Ok(None);
        }
        let lifecycle_lease = if let Some(managed) = &self.inner.managed {
            match managed
                .lifecycle
                .acquire(LifecycleWorkKind::Timer, &managed.name, &managed.id)
            {
                Ok(lease) => Some(lease),
                Err(Error::Inactive(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        let next = advance_next(expected, self.inner.period, now.nanoseconds)?;
        state.last = Some(now.nanoseconds);
        state.next = Some(next);
        state.in_flight = true;
        Ok(Some(TimerCall {
            timer: self.inner.clone(),
            generation: state.generation,
            info: TimerInfo {
                expected_call_time: expected,
                actual_call_time: now.nanoseconds,
            },
            _lifecycle_lease: lifecycle_lease,
        }))
    }
    fn wait_and_take(&self) -> Result<TimerCall> {
        self.wait_and_take_with_check(None)
    }
    fn wait_and_take_with_check(
        &self,
        mut check: Option<&mut dyn FnMut() -> Result<()>>,
    ) -> Result<TimerCall> {
        loop {
            // The caller may run a language signal handler, including one
            // that closes this timer. No clock/timer/lifecycle lock is held.
            if let Some(check) = &mut check {
                check()?;
            }
            if !self.inner.business_active()? {
                return Err(Error::Inactive("managed Rate is not active".into()));
            }
            let clock_state = self.inner.clock.inner.state.lock().unwrap();
            let now = self.inner.clock.inner.now_locked(&clock_state)?;
            let mut state = self.inner.state.lock().unwrap();
            if state.canceled {
                return Err(Error::InvalidArgument("timer is canceled".into()));
            }
            if let Some(call) = self.take_locked(&mut state, now)? {
                return Ok(call);
            }
            let mut wait = if now.use_sim_time || !now.initialized || state.in_flight {
                // A lifecycle gate can close without a clock update. Poll only
                // managed Rate waiters so they observe that control transition.
                self.inner
                    .managed
                    .as_ref()
                    .map(|_| Duration::from_millis(20))
            } else {
                let left = state
                    .next
                    .unwrap_or(now.nanoseconds)
                    .saturating_sub(now.nanoseconds)
                    .max(0) as u64;
                // A system clock can jump without a ROS override notification.
                Some(Duration::from_nanos(left).min(Duration::from_millis(100)))
            };
            if check.is_some() {
                let interval = Duration::from_millis(50);
                wait = Some(wait.map_or(interval, |wait| wait.min(interval)));
            }
            drop(clock_state);
            if let Some(wait) = wait {
                drop(self.inner.changed.wait_timeout(state, wait).unwrap());
            } else {
                drop(self.inner.changed.wait(state).unwrap());
            }
        }
    }
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }
    pub(crate) fn shutdown_resource(&self) -> Arc<dyn ShutdownResource> {
        self.inner.clone()
    }
}
impl TimerInner {
    fn business_active(&self) -> Result<bool> {
        self.managed.as_ref().map_or(Ok(true), |managed| {
            managed
                .lifecycle
                .active_epoch()
                .map(|epoch| epoch.is_some())
        })
    }
    fn on_jump(&self, before: Option<i64>, after: Option<i64>, source_change: bool) {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        if source_change {
            state.generation = state.generation.wrapping_add(1);
            state.credit = state
                .next
                .zip(before)
                .map(|(next, now)| next.saturating_sub(now));
            state.last = None;
            state.next = None;
        }
        if let Some(now) = after {
            if state.next.is_none() {
                if let Some(credit) = state.credit.take().filter(|credit| *credit != 0) {
                    // Preserve the *actual* fixed Jazzy formula, not a stronger
                    // "remaining duration" interpretation of its comment.
                    let last = now.saturating_sub(credit);
                    state.last = Some(last);
                    state.next = last.checked_add(self.period);
                } else {
                    state.last = Some(now);
                    state.next = now.checked_add(self.period);
                }
            } else if state.last.is_some_and(|last| now < last) {
                state.generation = state.generation.wrapping_add(1);
                state.last = Some(now);
                state.next = now.checked_add(self.period);
            } else if before.is_some_and(|before| now < before) {
                // A smaller rollback keeps the fixed next-call timestamp but
                // invalidates a callback that was queued in the old epoch.
                state.generation = state.generation.wrapping_add(1);
            }
            if state.next.is_none() {
                state.closed = true;
            }
        }
        self.changed.notify_all();
    }
    fn close(&self) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(Error::Closed);
        }
        state.closed = true;
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        Ok(())
    }
}
impl ShutdownResource for TimerInner {
    fn shutdown(&self) {
        let _ = self.close();
    }
    fn prepare_lifecycle_activation(&self) -> Result<()> {
        if self.managed.is_none() {
            return Ok(());
        }
        let clock_state = self.clock.inner.state.lock().unwrap();
        let now = self.clock.inner.now_locked(&clock_state)?;
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Ok(());
        }
        // Paused periods do not form a backlog. Existing leases remain valid
        // when a failed deactivation returns to active.
        state.last = now.initialized.then_some(now.nanoseconds);
        state.next = state
            .last
            .map(|now| now.checked_add(self.period).ok_or_else(time_overflow))
            .transpose()?;
        state.credit = None;
        self.changed.notify_all();
        Ok(())
    }
}

#[derive(Clone)]
pub struct Rate {
    timer: Timer,
}
impl Rate {
    pub fn new(clock: Clock, frequency: f64) -> Result<Self> {
        Self::build(clock, frequency, None)
    }
    pub(crate) fn new_managed(
        clock: Clock,
        frequency: f64,
        lifecycle: Lifecycle,
        node_name: &str,
    ) -> Result<Self> {
        Self::build(clock, frequency, Some((lifecycle, node_name)))
    }
    fn build(clock: Clock, frequency: f64, managed: Option<(Lifecycle, &str)>) -> Result<Self> {
        if !frequency.is_finite() || frequency <= 0.0 {
            return Err(Error::InvalidArgument(
                "frequency must be finite and positive".into(),
            ));
        }
        let period = Duration::try_from_secs_f64(1.0 / frequency)
            .map_err(|_| Error::InvalidArgument("frequency period overflow".into()))?;
        if period.is_zero() {
            return Err(Error::InvalidArgument(
                "frequency exceeds nanosecond resolution".into(),
            ));
        }
        Ok(Self {
            timer: if let Some((lifecycle, name)) = managed {
                Timer::new_managed(clock, period, true, lifecycle, name)?
            } else {
                Timer::new(clock, period, true)?
            },
        })
    }
    pub fn sleep(&self) -> Result<()> {
        loop {
            let call = self.timer.wait_and_take()?;
            if call.is_valid() {
                return Ok(());
            }
        }
    }
    /// Wait for the same periodic timer, checking caller interruption outside
    /// locks at intervals of at most 50 ms. An error from check interrupts only
    /// this sleep; it neither closes the rate nor advances its next period.
    /// The check itself must not block.
    pub fn sleep_with_check(&self, mut check: impl FnMut() -> Result<()>) -> Result<()> {
        loop {
            let call = self.timer.wait_and_take_with_check(Some(&mut check))?;
            if call.is_valid() {
                return Ok(());
            }
        }
    }
    pub fn close(&self) -> Result<()> {
        self.timer.close()
    }
    pub(crate) fn timer(&self) -> &Timer {
        &self.timer
    }
}

fn duration_ns(duration: Duration) -> Result<i64> {
    i64::try_from(duration.as_nanos()).map_err(|_| time_overflow())
}
fn time_overflow() -> Error {
    Error::InvalidArgument("nanosecond time overflow".into())
}
fn advance_next(expected: i64, period: i64, now: i64) -> Result<i64> {
    if period == 0 {
        return Ok(now);
    }
    let first = expected.checked_add(period).ok_or_else(time_overflow)?;
    if first > now {
        return Ok(first);
    }
    let periods = 1 + (now - first) / period;
    first
        .checked_add(periods.checked_mul(period).ok_or_else(time_overflow)?)
        .ok_or_else(time_overflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn simulated() -> Clock {
        let clock = Clock::new(ClockType::Ros);
        clock.set_use_sim_time(true).unwrap();
        clock
    }
    #[test]
    fn zero_is_initialized_and_forward_progress_preserves_overrun_readiness() {
        let clock = simulated();
        let timer = Timer::new(clock.clone(), Duration::from_nanos(10), true).unwrap();
        assert!(!clock.now().unwrap().initialized);
        assert!(timer.take_ready().unwrap().is_none());
        clock.update_ros_time(0).unwrap();
        assert!(clock.now().unwrap().initialized);
        assert_eq!(
            timer.time_until_next_call().unwrap(),
            Some(Duration::from_nanos(10))
        );
        clock.update_ros_time(100).unwrap();
        let call = timer.take_ready().unwrap().unwrap();
        assert_eq!(call.info().expected_call_time, 10);
        assert_eq!(call.info().actual_call_time, 100);
        for _ in 0..10 {
            clock.update_ros_time(100).unwrap();
            assert!(timer.take_ready().unwrap().is_none());
        }
        clock.update_ros_time(200).unwrap();
        assert!(call.is_valid());
        call.finish();
        // One overrun remains ready, as with a steady/system timer. Taking it
        // advances next beyond now; missed periods never become a callback list.
        let overrun = timer.take_ready().unwrap().unwrap();
        assert_eq!(overrun.info().expected_call_time, 110);
        assert_eq!(overrun.info().actual_call_time, 200);
        overrun.finish();
        assert!(timer.take_ready().unwrap().is_none());
        clock.update_ros_time(210).unwrap();
        assert!(timer.take_ready().unwrap().is_some());
    }
    #[test]
    fn consecutive_forward_samples_coalesce_without_evicting_other_transitions() {
        let clock = simulated();
        clock.update_ros_time(0).unwrap();
        clock.update_ros_time(10).unwrap();
        clock.update_ros_time(5).unwrap();
        for value in 6..=2_000 {
            clock.update_ros_time(value).unwrap();
        }
        let events = clock.take_jump_events();
        assert_eq!(
            events.iter().map(|e| e.reason.as_str()).collect::<Vec<_>>(),
            vec![
                "ros_time_activated",
                "initialized",
                "forward",
                "backward",
                "forward"
            ]
        );
        assert_eq!(clock.forward_update_count(), 1 + 1_995);
        assert_eq!(
            (events[2].before, events[2].after, events[2].delta),
            (Some(0), Some(10), Some(10))
        );
        assert_eq!(
            (events[4].before, events[4].after, events[4].delta),
            (Some(5), Some(2_000), Some(1_995))
        );
        assert_eq!(events[4].sequence, 5);
    }
    #[test]
    fn dropped_timers_are_pruned_when_registering_on_nonjumping_clocks() {
        for kind in [ClockType::Steady, ClockType::System, ClockType::Ros] {
            let clock = Clock::new(kind);
            for _ in 0..1_000 {
                drop(Timer::new(clock.clone(), Duration::from_secs(1), true).unwrap());
            }
            let live = Timer::new(clock.clone(), Duration::from_secs(1), true).unwrap();
            let state = clock.inner.state.lock().unwrap();
            assert_eq!(state.timers.len(), 1);
            assert_eq!(state.timers[0].strong_count(), 1);
            drop(state);
            drop(live);
        }
    }
    #[test]
    fn slow_callbacks_leave_one_ready_lease_on_steady_and_simulated_clocks() {
        let steady = Clock::new(ClockType::Steady);
        let steady_timer = Timer::new(steady.clone(), Duration::from_millis(1), true).unwrap();
        let first = steady_timer.wait_and_take().unwrap();
        let next = steady_timer.inner.state.lock().unwrap().next.unwrap();
        while steady.now().unwrap().nanoseconds < next + 1_000_000 {
            std::thread::yield_now();
        }
        assert!(steady_timer.take_ready().unwrap().is_none());
        first.finish();
        let overrun = steady_timer.take_ready().unwrap().unwrap();
        assert_eq!(overrun.info().expected_call_time, next);
        overrun.finish();

        let sim = simulated();
        sim.update_ros_time(0).unwrap();
        let sim_timer = Timer::new(sim.clone(), Duration::from_millis(1), true).unwrap();
        sim.update_ros_time(1_000_000).unwrap();
        let first = sim_timer.take_ready().unwrap().unwrap();
        sim.update_ros_time(3_000_000).unwrap();
        assert!(sim_timer.take_ready().unwrap().is_none());
        first.finish();
        let overrun = sim_timer.take_ready().unwrap().unwrap();
        assert_eq!(overrun.info().expected_call_time, 2_000_000);
        overrun.finish();
        assert!(sim_timer.take_ready().unwrap().is_none());
    }
    #[test]
    fn rollback_and_cancel_invalidate_queued_lease_without_second_inflight() {
        let clock = simulated();
        clock.update_ros_time(20).unwrap();
        let timer = Timer::new(clock.clone(), Duration::from_nanos(5), true).unwrap();
        clock.update_ros_time(25).unwrap();
        let call = timer.take_ready().unwrap().unwrap();
        clock.update_ros_time(10).unwrap();
        assert!(!call.is_valid());
        clock.update_ros_time(15).unwrap();
        assert!(timer.take_ready().unwrap().is_none());
        drop(call);
        let call = timer.take_ready().unwrap().unwrap();
        timer.cancel().unwrap();
        assert!(!call.is_valid());
        timer.reset().unwrap();
        clock.update_ros_time(20).unwrap();
        assert!(timer.take_ready().unwrap().is_none());
        drop(call);
        assert!(timer.take_ready().unwrap().is_some());
    }
    #[test]
    fn jazzy_source_change_uses_period_minus_saved_credit_formula() {
        let clock = simulated();
        clock.update_ros_time(1_000).unwrap();
        let timer = Timer::new(clock.clone(), Duration::from_nanos(500), true).unwrap();
        clock.update_ros_time(1_100).unwrap();
        // rcl timer.c:107-111; credit=1500-1100=400, new next=new now+100.
        timer.inner.on_jump(Some(1_100), Some(10_000), true);
        let state = timer.inner.state.lock().unwrap();
        assert_eq!(state.last, Some(9_600));
        assert_eq!(state.next, Some(10_100));
    }
    #[test]
    fn source_switch_invalidates_queued_call_and_parallel_executors_get_one_lease() {
        let clock = simulated();
        clock.update_ros_time(0).unwrap();
        let timer = Timer::new(clock.clone(), Duration::from_nanos(1), true).unwrap();
        clock.update_ros_time(1).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(17));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let timer = timer.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let lease = timer.take_ready().unwrap();
                    let acquired = lease.is_some();
                    barrier.wait();
                    drop(lease);
                    acquired
                })
            })
            .collect();
        barrier.wait();
        assert_eq!(
            workers
                .into_iter()
                .map(|worker| usize::from(worker.join().unwrap()))
                .sum::<usize>(),
            1
        );
        clock.update_ros_time(2).unwrap();
        let lease = timer.take_ready().unwrap().unwrap();
        clock.set_use_sim_time(false).unwrap();
        assert!(!lease.is_valid());
        assert!(timer.take_ready().unwrap().is_none());
        clock.set_use_sim_time(true).unwrap();
        assert!(!lease.is_valid());
        drop(lease);
    }
    #[test]
    fn rate_blocks_on_sim_pause_and_close_wakes_without_executor() {
        let clock = simulated();
        clock.update_ros_time(0).unwrap();
        let rate = Rate::new(clock.clone(), 1.0).unwrap();
        let worker = rate.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || tx.send(worker.sleep()).unwrap());
        assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
        clock.update_ros_time(1_000_000_000).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), Ok(()));
        thread.join().unwrap();
        let worker = rate.clone();
        let thread = std::thread::spawn(move || worker.sleep());
        rate.close().unwrap();
        assert_eq!(thread.join().unwrap(), Err(Error::Closed));
    }
    #[test]
    fn rate_interruption_keeps_the_same_unconsumed_period() {
        let clock = simulated();
        clock.update_ros_time(0).unwrap();
        let rate = Rate::new(clock.clone(), 1.0).unwrap();
        let caller = std::thread::current().id();
        let mut checks = 0;
        let interruption = Error::Transport("caller interruption".into());
        assert_eq!(
            rate.sleep_with_check(|| {
                assert_eq!(std::thread::current().id(), caller);
                checks += 1;
                if checks == 3 {
                    Err(interruption.clone())
                } else {
                    Ok(())
                }
            }),
            Err(interruption)
        );
        assert_eq!(checks, 3);
        assert_eq!(
            rate.timer.time_until_next_call().unwrap(),
            Some(Duration::from_secs(1))
        );
        clock.update_ros_time(1_000_000_000).unwrap();
        rate.sleep().unwrap();
    }
    #[test]
    fn rate_check_can_close_the_rate_and_preserve_the_callers_error() {
        let rate = Rate::new(simulated(), 1.0).unwrap();
        let interruption = Error::Transport("caller interruption after close".into());
        assert_eq!(
            rate.sleep_with_check(|| {
                rate.close().unwrap();
                Err(interruption.clone())
            }),
            Err(interruption)
        );
        assert_eq!(rate.sleep(), Err(Error::Closed));
    }
    #[test]
    fn zero_period_is_single_inflight_and_seconds_overflow_is_rejected() {
        let clock = simulated();
        clock.update_ros_time(0).unwrap();
        let timer = Timer::new(clock.clone(), Duration::ZERO, true).unwrap();
        let call = timer.take_ready().unwrap().unwrap();
        assert!(timer.take_ready().unwrap().is_none());
        drop(call);
        assert!(timer.take_ready().unwrap().is_some());
        for frequency in [0.0, -1.0, f64::NAN, f64::INFINITY, 1e20, 1e-30] {
            assert!(Rate::new(clock.clone(), frequency).is_err());
        }
        assert!(Timer::new(clock, Duration::MAX, true).is_err());
    }
}
