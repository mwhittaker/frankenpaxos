// The frankenpaxosjs namespace.
frankenpaxosjs = {}

Vue.mixin({
  data: function() {
    return {
      JsUtils: frankenpaxos.JsUtils,
    }
  }
});

frankenpaxosjs.frankenpaxos_transport_timer = {
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

frankenpaxosjs.frankenpaxos_transport_buffered_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_buffered', this.message);
  },
};

frankenpaxosjs.frankenpaxos_transport_staged_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_staged', this.message);
  },
};

Vue.component('frankenpaxos-transport', {
  props: [
    'transport',
    'callbacks',
    // 'timer_started',
    // 'timer_stopped',
    // 'message_buffered',
    // 'message_staged',
  ],

  components: {
    'frankenpaxos-transport-timer': frankenpaxosjs.frankenpaxos_transport_timer,
    'frankenpaxos-transport-buffered-message': frankenpaxosjs.frankenpaxos_transport_buffered_message,
    'frankenpaxos-transport-staged-message': frankenpaxosjs.frankenpaxos_transport_staged_message,
  },

  // TODO: Add keys.
  template: `
    <div hidden=true>
      <frankenpaxos-transport-timer
        v-for="timer in timers"
        :timer="timer"
        v-on:timer_started="callbacks.timer_started"
        v-on:timer_stopped="callbacks.timer_stopped">
      </frankenpaxos-transport-timer>
      <frankenpaxos-transport-buffered-message
        v-for="message in buffered_messages"
        :message="message"
        v-on:message_buffered="callbacks.message_buffered">
      </frankenpaxos-transport-buffered-message>
      <frankenpaxos-transport-staged-message
        v-for="message in staged_messages"
        :message="message"
        v-on:message_staged="callbacks.message_staged">
      </frankenpaxos-transport-staged-message>
    </div>
  `,

  computed: {
    timers: function() {
      return this.JsUtils.seqToJs(this.transport.timers);
    },

    buffered_messages: function() {
      return this.JsUtils.seqToJs(this.transport.bufferedMessages);
    },

    staged_messages: function() {
      return this.JsUtils.seqToJs(this.transport.stagedMessages);
    },
  },
});

Vue.component('frankenpaxos-simulated-app', {
  props: [
    'transport',
    // (message, callback) -> ().
    'send_message',
  ],

  template: `
    <frankenpaxos-transport
      :transport="transport"
      :callbacks="callbacks">
    </frankenpaxos-transport>
  `,

  data: function() {
    return {
      timers: {},
      callbacks: {
        timer_started: (timer) => {
          // If we reset a timer, it toggles from not running to running very
          // quickly. When this happens, Vue does not always trigger an event
          // for the stopping and starting of the timer. It usually just
          // triggers an event for the starting. Thus, if we start a timer that
          // is already started, we should cancel it first.
          if ([timer.address, timer.name()] in this.timers) {
            clearTimeout(this.timers[[timer.address, timer.name()]]);
            delete this.timers[[timer.address, timer.name()]];
          }

          this.timers[[timer.address, timer.name()]] = setTimeout(() => {
            timer.run();
          }, timer.delayMilliseconds());
        },
        timer_stopped: (timer) => {
          if ([timer.address, timer.name()] in this.timers) {
            clearTimeout(this.timers[[timer.address, timer.name()]]);
            delete this.timers[[timer.address, timer.name()]];
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

Vue.component('frankenpaxos-clickthrough-app', {
  props: [
    'transport',
    // (message, callback) -> ().
    'send_message',
  ],

  template: `
    <frankenpaxos-transport
      :transport="transport"
      :callbacks="callbacks">
    </frankenpaxos-transport>
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

Vue.component('frankenpaxos-log', {
  // log is a list of JsLogEntry. See JsLogger.scala for more information on
  // JsLogEntry.
  props: ['log'],
  updated: function() {
    this.$el.scrollTop = this.$el.scrollHeight;
  },
  template: `
    <div class="frankenpaxos-log">
      <div v-for="log_entry in log" class="frankenpaxos-log-entry">
        <span v-bind:class="'frankenpaxos-log-' + log_entry.typ.toString()">
          [{{log_entry.typ.toString()}}]
        </span>
        {{log_entry.text}}
      </div>
    </div>
  `
});


Vue.component('frankenpaxos-timers', {
  // timers is a list of JsTransportTimer. See JsTransport.scala for more
  // information on JsTransportTimer.
  props: ['timers'],
  template: `
    <div class="frankenpaxos-timers">
      <div v-for="timer in timers">
        <button class="frankenpaxos-button frankenpaxos-timers-trigger"
           v-bind:disabled="!timer.running"
           v-on:click="timer.run()">Trigger</button>
        <span class="frankenpaxos-timers-name">{{timer.name()}}</span>
      </div>
    </div>
  `
});


// TODO: Right now, clicking `drop` on a message drops the first instance of
// the message instead of the instance of the message that was clicked. Fix
// this.
Vue.component("frankenpaxos-staged-messages", {
  props: ["transport", "actor", "messages"],
  template: `
    <div class="frankenpaxos-messages">
      <div v-for="message in messages">
        <div class="frankenpaxos-messages-message">
          <button class="frankenpaxos-button frankenpaxos-messages-deliver"
             v-on:click="transport.deliverMessage(message)">
            Deliver</button>
          <button class="frankenpaxos-button frankenpaxos-messages-drop"
             v-on:click="transport.dropMessage(message)">
            Drop</button>
          <button class="frankenpaxos-button frankenpaxos-messages-duplicate"
             v-on:click="transport.stageMessage(message, false)">
            Duplicate</button>
          <span class="frankenpaxos-messages-src">from {{message.src.address}}</span>
          <div class="frankenpaxos-messages-text">
            {{actor.serializer.toPrettyString(
                actor.serializer.fromBytes(message.bytes))}}
          </div>
        </div>
      </div>
    </div>
  `
});

Vue.component('frankenpaxos-unittest', {
  props: ['transport'],
  template: `
    <div class="frankenpaxos-unittest">
      <div v-for="line in JsUtils.seqToJs(transport.unitTest())">
        <span class="frankenpaxos-unittest-line">
          {{line}}
        </span>
      </div>
    </div>
  `
});
