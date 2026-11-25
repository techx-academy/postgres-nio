import NIOConcurrencyHelpers
import NIOCore

/// The pool's clock: ``ContinuousClock``'s time, its sleeps scheduled on an event loop.
///
/// The pool sleeps once for every timer it starts and cancels most of them (an idle connection's
/// keep-alive and idle timers end when it is leased again). A cancelled `Task.sleep` keeps its
/// timer and task until the deadline it was given, so a busy pool held thousands of them; a
/// sleep here cancels its scheduled task when it is cancelled, and nothing is left.
@available(macOS 13.0, iOS 16.0, tvOS 16.0, watchOS 9.0, *)
struct PoolClock: Clock {
    typealias Instant = ContinuousClock.Instant
    typealias Duration = Swift.Duration

    let eventLoopGroup: any EventLoopGroup

    var now: Instant { ContinuousClock.now }
    var minimumResolution: Duration { ContinuousClock().minimumResolution }

    func sleep(until deadline: Instant, tolerance: Duration? = nil) async throws {
        let delay = ContinuousClock.now.duration(to: deadline)
        guard delay > .zero else { return try Task.checkCancellation() }
        let (seconds, attoseconds) = delay.components
        let amount = TimeAmount.nanoseconds(seconds * 1_000_000_000 + attoseconds / 1_000_000_000)
        let eventLoop = self.eventLoopGroup.any()
        let state = NIOLockedValueBox(SleepState())
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, any Error>) in
                let cancelled = state.withLockedValue { sleeping -> Bool in
                    if sleeping.cancelled { return true }
                    sleeping.continuation = continuation
                    sleeping.scheduled = eventLoop.scheduleTask(in: amount) {
                        let continuation = state.withLockedValue { state in
                            defer { state.continuation = nil }
                            return state.continuation
                        }
                        continuation?.resume()
                    }
                    return false
                }
                if cancelled { continuation.resume(throwing: CancellationError()) }
            }
        } onCancel: {
            let (continuation, scheduled) = state.withLockedValue { state in
                state.cancelled = true
                defer { (state.continuation, state.scheduled) = (nil, nil) }
                return (state.continuation, state.scheduled)
            }
            scheduled?.cancel()
            continuation?.resume(throwing: CancellationError())
        }
    }

    private struct SleepState {
        var cancelled = false
        var continuation: CheckedContinuation<Void, any Error>?
        var scheduled: Scheduled<Void>?
    }
}
