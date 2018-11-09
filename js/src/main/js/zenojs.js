// The zenojs namespace.
zenojs = {}

zenojs.zeno_transport_timer = {
  props: ['timer'],

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
};

zenojs.zeno_transport_buffered_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_buffered', this.message);
  },
};

zenojs.zeno_transport_staged_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_staged', this.message);
  },
};

Vue.component('zeno-transport', {
  props: [
    'transport',
    'callbacks',
    // 'timer_started',
    // 'timer_stopped',
    // 'message_buffered',
    // 'message_staged',
  ],

  components: {
    'zeno-transport-timer': zenojs.zeno_transport_timer,
    'zeno-transport-buffered-message': zenojs.zeno_transport_buffered_message,
    'zeno-transport-staged-message': zenojs.zeno_transport_staged_message,
  },

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
        <button class="zeno-button zeno-timers-trigger"
           v-bind:disabled="!timer.running"
           v-on:click="timer.run()">Trigger</button>
        <span class="zeno-timers-name">{{timer.name()}}</span>
      </div>
    </div>
  `
});


// TODO: Right now, clicking `drop` on a message drops the first instance of
// the message instead of the instance of the message that was clicked. Fix
// this.
Vue.component("zeno-staged-messages", {
  props: ["transport", "actor", "messages"],
  template: `
    <div class="zeno-messages">
      <div v-for="message in messages">
        <div class="zeno-messages-message">
          <button class="zeno-button zeno-messages-deliver"
             v-on:click="transport.deliverMessage(message)">
            Deliver</button>
          <button class="zeno-button zeno-messages-drop"
             v-on:click="transport.dropMessage(message)">
            Drop</button>
          <button class="zeno-button zeno-messages-duplicate"
             v-on:click="transport.stageMessage(message, false)">
            Duplicate</button>
          <span class="zeno-messages-src">from {{message.src.address}}</span>
          <div class="zeno-messages-text">
            {{actor.serializer.toPrettyString(
                actor.serializer.fromBytes(message.bytes))}}
          </div>
        </div>
      </div>
    </div>
  `
});
