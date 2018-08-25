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

// config_message(app, Message) -> {svg_message: svg object, animate_args, animate args, timeout, drop: }[drop]
// on_receive(app, Message)
// on_timer_start(app, Message)
// on_timer_stop(app, Message)
z.AnimatedApp = class AnimatedApp {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;

    let app_callbacks = {
      on_send: (app, message) => {
        let config = this.callbacks.config_message(this, message);
        if (config.drop) {
          return;
        }
        config.svg_message.animate(config.animate_args, config.timeout, () => {
          this.callbacks.on_receive(this, message);
          config.svg_message.remove();
        });
      },
      on_timer_start: (app, timer) => {
        this.callbacks.on_timer_start(this, timer);
      },
      on_timer_stop: (app, timer) => {
        this.callbacks.on_timer_stop(this, timer);
      },
    };
    this.app = new z.App(transport, app_callbacks);
    this.app.refresh();
  }

  deliver_message(message) {
    this.transport.deliverMessage(message);
    this.app.refresh();
  }

  run_timer(timer) {
    timer.stop();
    timer.run();
    this.app.refresh();
  }
}

// config_message(app, Message) -> {svg_message: svg object, animate_args, animate args, timeout, drop: }[drop]
// on_timer_start(app, Timer)
// on_timer_stop(app, Timer)
z.SimulatedApp = class SimulatedApp {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;
    this.timers = {};

    let animated_app_callbacks = {
      config_message: this.callbacks.config_message,
      on_receive: (animated_app, message) => {
        animated_app.deliver_message(message);
      },
      on_timer_start: (animated_app, timer) => {
        if (!(timer.address in this.timers)) {
          this.timers[timer.address] = {}
        }

        this.timers[timer.address][timer.name()] = setTimeout(() => {
          animated_app.run_timer(timer);
          this.callbacks.on_timer_start(timer);
        }, timer.delayMilliseconds());
      },
      on_timer_stop: (animated_app, timer) => {
        clearTimeout(this.timers[timer.address][timer.name()]);
        this.callbacks.on_timer_stop(timer);
      },
    };
    this.animated_app = new z.AnimatedApp(transport, animated_app_callbacks);
  }
}
