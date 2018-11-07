// The zenojs namespace.
let zenojs = {};

Vue.component('zeno-transport-timer', {
  props: ['timer'],

  // TODO: Remove.
  template: "<div></div>",

  created: function() {
    if (this.timer.running) {
      this.$emit('timer_started', this.timer);
    }
  },

  watch: {
    timer: {
      handler: function(timer) {
        if (timer.running) {
          this.$emit('timer_started', this.timer);
        } else {
          this.$emit('timer_stopped', this.timer);
        }
      },
      deep: true,
    }
  }
});

Vue.component('zeno-transport-buffered-message', {
  props: ['message'],

  template: "<div></div>",

  created: function() {
    this.$emit('message_buffered', this.message);
  },
});

Vue.component('zeno-transport-staged-message', {
  props: ['message'],

  template: "<div></div>",

  created: function() {
    this.$emit('message_staged', this.message);
  },
});

Vue.component('zeno-transport', {
  props: [
    'transport',
    'callbacks',
    // 'timer_started',
    // 'timer_stopped',
    // 'message_buffered',
    // 'message_staged',
  ],

  // TODO: Add keys.
  template: `
    <div hidden=true>
      <zeno-transport-timer
        v-for="timer in timers"
        :timer="timer"
        v-on:timer_started="callbacks.timer_started"
        v-on:timer_stopped="callbacks.timer_stopped">
      </zeno-transport-timer>
      <zeno-transport-buffered-message
        v-for="message in buffered_messages"
        :message="message"
        v-on:message_buffered="callbacks.message_buffered">
      </zeno-transport-buffered-message>
      <zeno-transport-staged-message
        v-for="message in staged_messages"
        :message="message"
        v-on:message_staged="callbacks.message_staged">
      </zeno-transport-staged-message>
    </div>
  `,

  computed: {
    timers: function() {
      return this.transport.timersJs();
    },

    buffered_messages: function() {
      return this.transport.bufferedMessagesJs();
    },

    staged_messages: function() {
      return this.transport.stagedMessagesJs();
    },
  },
});

Vue.component('zeno-simulated-app', {
  props: [
    'transport',
    // (message, callback) -> ().
    'send_message',
  ],

  template: `
    <zeno-transport
      :transport="transport"
      :callbacks="callbacks">
    </zeno-transport>
  `,

  data: function() {
    return {
      timers: {},
      callbacks: {
        timer_started: (timer) => {
          if (!(timer in this.timers)) {
            this.timers[[timer.address, timer.name()]] = setTimeout(() => {
              timer.run();
            }, timer.delayMilliseconds());
          }
        },
        timer_stopped: (timer) => {
          if (timer in this.timers) {
            clearTimeout(this.timers[[timer.address, timer.name()]]);
          }
        },
        message_buffered: (message) => {
          this.send_message(message, () => {
            this.transport.stageMessage(message);
          });
        },
        message_staged: (message) => {
          this.transport.deliverMessage(message);
        },
      }
    }
  }
});

Vue.component('zeno-clickthrough-app', {
  props: [
    'transport',
    // (message, callback) -> ().
    'send_message',
  ],

  template: `
    <zeno-transport
      :transport="transport"
      :callbacks="callbacks">
    </zeno-transport>
  `,

  data: function() {
    return {
      timers: {},
      callbacks: {
        timer_started: (timer) => {},
        timer_stopped: (timer) => {},
        message_buffered: (message) => {
          this.send_message(message, () => {
            this.transport.stageMessage(message);
          });
        },
        message_staged: (message) => {},
      }
    }
  }
});

Vue.component('zeno-log', {
  // log is a list of JsLogEntry. See JsLogger.scala for more information on
  // JsLogEntry.
  props: ['log'],
  updated: function() {
    this.$el.scrollTop = this.$el.scrollHeight;
  },
  template: `
    <div class="zeno-log">
      <div v-for="log_entry in log" class="zeno-log-entry">
        <span v-bind:class="'zeno-log-' + log_entry.typ.toString()">
          [{{log_entry.typ.toString()}}]
        </span>
        {{log_entry.text}}
      </div>
    </div>
  `
});


Vue.component('zeno-timers', {
  // timers is a list of JsTransportTimer. See JsTransport.scala for more
  // information on JsTransportTimer.
  props: ['timers'],
  template: `
    <div class="zeno-timers">
      <div v-for="timer in timers">
        <a class="zeno-button zeno-timers-trigger"
           v-bind:disabled="!timer.running"
           v-on:click="$emit('timer-trigger', timer)">Trigger</a>
        <span class="zeno-timers-name">{{timer.name()}}</span>
      </div>
    </div>
  `
});

Vue.component('zeno-simulated-node', {
  // node is an object {log} with fields described in the components above.
  props: ['node'],

  template: `
    <div class="zeno-node">
      <slot></slot>

      <div class="zeno-box">
        <h3 class="zeno-box-title">Log</h3>
        <zeno-log v-bind:log='node.actor.logger.logJs()'></zeno-log>
      </div>
    </div>
  `
});

Vue.component('zeno-messages', {
  props: [
    // actor is an Actor.
    'actor',

    // messages is a list of JsTransport.Message. See JsTransport.scala for
    // more information on JsTransport.Message.
    'messages',
  ],
  template: `
    <div class="zeno-messages">
      <div v-for="(message, index) in messages">
        <div class="zeno-messages-message">
          <a class="zeno-button zeno-messages-deliver"
             v-on:click="$emit('message-deliver', {message:message, index:index})">
            Deliver</a>
          <a class="zeno-button zeno-messages-drop"
             v-on:click="$emit('message-drop', {message:message, index:index})">
            Drop</a>
          <a class="zeno-button zeno-messages-duplicate"
             v-on:click="$emit('message-duplicate', {message:message, index:index})">
            Duplicate</a>
          <span class="zeno-messages-src">from {{message.src.address}}</span>
          <div class="zeno-messages-text">
            {{actor.parseInboundMessageToString(message.bytes)}}
          </div>
        </div>
      </div>
    </div>
  `
});

Vue.component('zeno-clickthrough-node', {
  // node is an object {actor; log; timers; messages} with fields described in
  // the components above.
  props: ['node'],
  template: `
    <div class="zeno-node">
      <slot></slot>

      <div class="zeno-box">
        <h3 class="zeno-box-title">Log</h3>
        <zeno-log v-bind:log='node.log'></zeno-log>
      </div>

      <hr class="zeno-hr"></hr>

      <div class="zeno-box">
        <h3 class="zeno-box-title">Timers</h3>
        <zeno-timers
          v-bind:timers='node.timers'
          v-on:timer-trigger='$emit("timer-trigger", $event)'>
        </zeno-timers>
      </div>

      <hr class="zeno-hr"></hr>

      <div class="zeno-box">
        <h3 class="zeno-box-title">Messages</h3>
        <zeno-messages
          v-bind:actor='node.actor'
          v-bind:messages='node.messages'
          v-on:message-deliver='$emit("message-deliver", $event)'
          v-on:message-drop='$emit("message-drop", $event)'
          v-on:message-duplicate='$emit("message-duplicate", $event)'>
        </zeno-messages>
      </div>
    </div>
  `
});



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

    // timers is of type Dict[JsTransportAddress, Dict[String, Timer]]. For
    // every timer `t` of actor `a`, we have `timers[a.address][t.name] == t`.
    this.timers = {};

    // timers maps an actor's address to a list of all the actor's received
    // (but not yet delivered) messages.
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
        this.timers[timer.address][timer.name] = timer;
        this.callbacks.on_timer_start(this, timer);
      },

      on_timer_stop: (animated_app, timer) => {
        if (!(timer.address in this.timers)) {
          this.timers[timer.address] = {};
        }
        this.timers[timer.address][timer.name] = timer;
        this.callbacks.on_timer_stop(this, timer);
      },
    };
    this.animated_app = new zenojs.AnimatedApp(transport, animated_app_callbacks);
  }

  get_timers(address) {
    return Object.values(this.timers[address]);
  }

  get_messages(address) {
    return this.messages[address];
  }

  run_timer(timer) {
    this.animated_app.run_timer(timer);
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
}

