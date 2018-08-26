// The zenojs namespace.
let zenojs = {};

// on_send(app, Message)
// on_timer_stop(app, Timer)
// on_timer_start(app, Timer)
zenojs.App = class App {
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

// config_message(app, Message) -> {
//  svg_message: svg object,
//  animate_args,
//  animate args,
//  timeout,
//  drop:
// }
//
// on_send(app, Message)
// on_deliver(app, Message)
// on_receive(app, Message)
// on_timer_start(app, Message)
// on_timer_stop(app, Message)
zenojs.AnimatedApp = class AnimatedApp {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;

    this.app = new zenojs.App(transport, {
      on_send: (app, message) => {
        let {svg_message, animate_args, timeout, drop} =
            this.callbacks.config_message(this, message);
        if (drop) {
          svg_message.remove();
        } else {
          svg_message.animate(animate_args, timeout, () => {
            this.callbacks.on_receive(this, message);
            svg_message.remove();
          });
        }
        this.callbacks.on_send(message);
      },
      on_timer_start: (app, timer) => {
        this.callbacks.on_timer_start(this, timer);
      },
      on_timer_stop: (app, timer) => {
        this.callbacks.on_timer_stop(this, timer);
      },
    });
    this.app.refresh();
  }

  deliver_message(message) {
    this.transport.deliverMessage(message);
    this.app.refresh();
    this.callbacks.on_deliver(this, message);
  }

  run_timer(timer) {
    timer.stop();
    timer.run();
    this.app.refresh();
  }
}

// config_message(app, Message) -> {svg_message: svg object, animate_args, animate args, timeout, drop: }[drop]
//
// on_send(app, Message)
// on_deliver(app, Message)
// on_timer_start(app, Message)
// on_timer_stop(app, Message)
zenojs.SimulatedApp = class SimulatedApp {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;
    this.timers = {};

    let animated_app_callbacks = {
      config_message: this.callbacks.config_message,
      on_send: (animated_app, message) => {
        this.callbacks.on_send(this, message);
      },
      on_receive: (animated_app, message) => {
        animated_app.deliver_message(message);
      },
      on_deliver: (animated_app, message) => {
        this.callbacks.on_deliver(this, message);
      },
      on_timer_start: (animated_app, timer) => {
        if (!(timer.address in this.timers)) {
          this.timers[timer.address] = {}
        }

        this.timers[timer.address][timer.name()] = setTimeout(() => {
          animated_app.run_timer(timer);
        }, timer.delayMilliseconds());
        this.callbacks.on_timer_start(timer);
      },
      on_timer_stop: (animated_app, timer) => {
        clearTimeout(this.timers[timer.address][timer.name()]);
        this.callbacks.on_timer_stop(timer);
      },
    };
    this.animated_app = new zenojs.AnimatedApp(transport, animated_app_callbacks);
  }
}

// config_message(app, Message) -> {svg_message: svg object, animate_args, animate args, timeout, drop: }[drop]
//
// on_send(app, Message)
// on_receive(app, Message)
// on_deliver(app, Message)
// on_timer_start(app, Message)
// on_timer_stop(app, Message)
zenojs.ClickthroughApp = class ClickthroughApp {
  constructor(transport, callbacks) {
    this.transport = transport;
    this.callbacks = callbacks;

    this.timers = {};
    this.messages = {};

    let animated_app_callbacks = {
      config_message: this.callbacks.config_message,

      on_send: (animated_app, message) => {
        this.callbacks.on_send(this, message);
      },

      on_receive: (animated_app, message) => {
        if (!(message.dst in this.messages)) {
          this.messages[message.dst] = [];
        }
        this.messages[message.dst].push(message);
        this.callbacks.on_receive(this, message);
      },

      on_deliver: (animated_app, message) => {
        this.callbacks.on_deliver(this, message);
      },

      on_timer_start: (animated_app, timer) => {
        if (!(timer.address in this.timers)) {
          this.timers[timer.address] = {};
        }
        this.timers[timer.address][timer.name] = {
          timer: timer,
          active: true,
        }

        this.callbacks.on_timer_start(this, timer);
      },

      on_timer_stop: (animated_app, timer) => {
        if (!(timer.address in this.timers)) {
          this.timers[timer.address] = {};
        }
        this.timers[timer.address][timer.name] = {
          timer: timer,
          active: false,
        }

        this.callbacks.on_timer_stop(this, timer);
      },
    };
    this.animated_app = new zenojs.AnimatedApp(transport, animated_app_callbacks);
  }

  deliver_message(message, index) {
    this.animated_app.deliver_message(message);
    this.drop_message(message, index);
  }

  drop_message(message, index) {
    this.messages[message.dst].splice(index, 1);
  }

  duplicate_message(message, index) {
    this.messages[message.dst].splice(index + 1, 0, message);
  }

  run_timer(timer) {
    this.animated_app.run_timer(timer);
  }
}
