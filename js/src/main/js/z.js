// The z namespace.
let z = {};

// on_send(app, Message)
// on_timer_stop(app, Timer)
// on_timer_start(app, Timer)

z.App = class App {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;
  }

  refresh() {
    // Process sent messages.
    for (let msg of this.transport.bufferedMessagesJs()) {
      this.callbacks.on_send(this, msg);
    }
    this.transport.clearBufferedMessages();

    // Process timers.
    for (let timer of this.transport.timersJs()) {
      // If the timer's version hasn't changed, then it hasn't been started or
      // stopped. Thus, we don't have to call any callbacks for it.
      if (timer.cached_version == timer.version) {
        continue;
      }

      // If the timer was previously running, then it must have been stopped
      // (maybe only temporarily).
      if (timer.cached_running) {
        this.callbacks.on_timer_stop(this, timer);
      }

      // If the timer is currently running, then it must have been started.
      if (timer.running) {
        this.callbacks.on_timer_start(this, timer);
      }

      timer.updateCache();
    }
  }
}
