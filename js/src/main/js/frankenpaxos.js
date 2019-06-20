// The frankenpaxosjs namespace.
frankenpaxosjs = {}

Vue.mixin({
  data: function() {
    return {
      JsUtils: frankenpaxos.JsUtils,
    }
  }
});


// Low-level transport wrapper. ////////////////////////////////////////////////
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

frankenpaxosjs.frankenpaxos_transport_timers = {
  props: [
    'timers',
    'timer_started',
    'timer_stopped',
  ],

  components: {
    'frankenpaxos-transport-timer': frankenpaxosjs.frankenpaxos_transport_timer,
  },

  data: function() {
    return {
      js_timers: frankenpaxos.JsUtils.seqToJs(this.timers),
    };
  },

  template: `
    <div>
      <div v-for="t in js_timers" :key="t.id">
        {{t.address}}
        {{t.name()}}
        {{t.id}}
        <frankenpaxos-transport-timer
          :timer="t"
          v-on:timer_started="timer_started"
          v-on:timer_stopped="timer_stopped">
        </frankenpaxos-transport-timer>
      </div>
    </div>
  `,

  watch: {
    timers: {
      handler: function(ts) {
        this.js_timers = frankenpaxos.JsUtils.seqToJs(ts);
      },
      deep: true,
    }
  },
};

frankenpaxosjs.frankenpaxos_transport_buffered_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_buffered', this.message);
  },
};

frankenpaxosjs.frankenpaxos_transport_buffered_messages = {
  props: [
    'buffered_messages',
    'message_buffered',
  ],

  components: {
    'frankenpaxos-transport-buffered-message':
      frankenpaxosjs.frankenpaxos_transport_buffered_message,
  },

  data: function() {
    return {
      js_buffered_messages:
        frankenpaxos.JsUtils.seqToJs(this.buffered_messages),
    };
  },

  template: `
    <div>
      <div v-for="m in js_buffered_messages" :key="m.id">
        <frankenpaxos-transport-buffered-message
          :message="m"
          v-on:message_buffered="message_buffered">
        </frankenpaxos-transport-buffered-message>
      </div>
    </div>
  `,

  watch: {
    buffered_messages: {
      handler: function(bm) {
        this.js_buffered_messages = frankenpaxos.JsUtils.seqToJs(bm);
      },
      deep: true,
    }
  },
};

frankenpaxosjs.frankenpaxos_transport_staged_message = {
  props: ['message'],
  template: "<div></div>",
  created: function() {
    this.$emit('message_staged', this.message);
  },
};

frankenpaxosjs.frankenpaxos_transport_staged_messages = {
  props: [
    'staged_messages',
    'message_staged',
  ],

  components: {
    'frankenpaxos-transport-staged-message':
      frankenpaxosjs.frankenpaxos_transport_staged_message,
  },

  data: function() {
    return {
      js_staged_messages: frankenpaxos.JsUtils.seqToJs(this.staged_messages),
    };
  },

  template: `
    <div>
      <div v-for="m in js_staged_messages" :key="m.id">
        <frankenpaxos-transport-staged-message
          :message="m"
          v-on:message_staged="message_staged">
        </frankenpaxos-transport-staged-message>
      </div>
    </div>
  `,

  watch: {
    staged_messages: {
      handler: function(bm) {
        this.js_staged_messages = frankenpaxos.JsUtils.seqToJs(bm);
      },
      deep: true,
    }
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
    'frankenpaxos-transport-timers':
      frankenpaxosjs.frankenpaxos_transport_timers,
    'frankenpaxos-transport-buffered-messages':
      frankenpaxosjs.frankenpaxos_transport_buffered_messages,
    'frankenpaxos-transport-staged-messages':
      frankenpaxosjs.frankenpaxos_transport_staged_messages,
  },

  template: `
    <div hidden="true">
      <frankenpaxos-transport-timers
        :timers="transport.timers"
        :timer_started="callbacks.timer_started"
        :timer_stopped="callbacks.timer_stopped">
      </frankenpaxos-transport-timers>

      <frankenpaxos-transport-buffered-messages
        :buffered_messages="transport.bufferedMessages"
        :message_buffered="callbacks.message_buffered">
      </frankenpaxos-transport-buffered-messages>

      <frankenpaxos-transport-staged-messages
        :staged_messages="transport.stagedMessages"
        :message_staged="callbacks.message_staged">
      </frankenpaxos-transport-staged-messages>
    </div>
  `,
});


// Applications. ///////////////////////////////////////////////////////////////
Vue.component('frankenpaxos-tweened-app', {
  props: {
    // A JsTransport.
    transport: Object,

    // A function of type JsTransportMessage -> TweenMax. Typically,
    // send_message would animate the sending of a message.
    send_message: Function,

    // An object with the following fields:
    //
    //   - time_scale: the time scale, or play rate, of the timeline. 2 is 2x
    //     real-time, and 0.5 is half real-time for example.
    //   - auto_deliver_messages: whether messages are automatically delivered
    //     to actors (true) or buffered (false).
    //   - auto_start_timers: whether actor message timers are started
    //   automatically.
    settings: Object,
  },

  data: function() {
    return {
      // This timeline sequences all timers and messages.
      timeline: new TimelineMax().timeScale(this.settings.time_scale),

      // timer_tweens[id] = TweenMax
      timer_tweens: {},

      callbacks: {
        timer_started: (timer) => {
          // If we reset a timer, it toggles from not running to running very
          // quickly. When this happens, Vue does not always trigger an event
          // for the stopping and starting of the timer. It usually just
          // triggers an event for the starting. Thus, if we start a timer that
          // is already started, we should cancel it first.
          if (timer.id in this.timer_tweens) {
            let tween = this.timer_tweens[timer.id];
            this.timeline.remove(tween);
            tween.kill()
            delete this.timer_tweens[timer.id];
          }

          let vm = this;
          let data = {
            time_elapsed: 0,
            progress: 0,
            timer: timer,
          }
          let tween = TweenMax.to(data, timer.delayMilliseconds() / 1000, {
            time_elapsed: timer.delayMilliseconds() / 1000,
            progress: 1,
            paused: !vm.settings.auto_start_timers,
            data: data,
            onComplete: function() {
              vm.timeline.remove(this);
              this.kill();
              delete vm.timer_tweens[timer.id];
              timer.run();
            },
            ease: Linear.easeNone,
          });
          this.timeline.add(tween, this.timeline.time());
          Vue.set(this.timer_tweens, timer.id, tween);
        },

        timer_stopped: (timer) => {
          if (timer.id in this.timer_tweens) {
            let tween = this.timer_tweens[timer.id];
            this.timeline.remove(tween);
            tween.kill()
            Vue.delete(this.timer_tweens, timer.id);
          }
        },

        message_buffered: (message) => {
          let tween = this.send_message(message);
          this.timeline.add(tween, this.timeline.time());

          let vm = this;
          let onComplete = tween.eventCallback('onComplete');
          tween.eventCallback('onComplete', function() {
            if (onComplete != null) {
              onComplete();
            }
            vm.timeline.remove(this);
            this.kill();
            vm.transport.stageMessage(message);
          });
        },

        message_staged: (message) => {
          if (this.settings.auto_deliver_messages) {
            this.transport.deliverMessage(message);
          }
        },
      }
    }
  },

  computed: {
    time_scale: function() {
      return this.settings.time_scale;
    },
  },

  watch: {
    time_scale: function() {
      if (this.settings.time_scale <= 0) {
        this.timeline.pause();
      } else {
        this.timeline.timeScale(this.settings.time_scale);
        if (this.timeline.paused()) {
          this.timeline.play();
        }
      }
    }
  },

  template: `
    <div>
      <frankenpaxos-transport
        :transport="transport"
        :callbacks="callbacks">
      </frankenpaxos-transport>
      <slot :timer_tweens="timer_tweens"></slot>
    </div>
  `,
});


// Visualizations. /////////////////////////////////////////////////////////////
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

Vue.component('frankenpaxos-timer', {
  // timer is a TweenMax with JsTransportTimer timer data.
  props: ['timer'],
  template: `
    <div style="display: inline-block;">
      <button class="frankenpaxos-button"
         v-bind:disabled="!timer.data.timer.running"
         v-on:click="timer.data.timer.run()">Trigger</button>
      <span>{{timer.data.timer.name()}}</span>
      <div class="timer-bar-outer"
           :style="{width: '1.5in'}">
        <div class="timer-bar-inner"
             :style="{width: 1.5 * timer.data.progress + 'in'}"></div>
      </div>
      <span>
        ({{timer.data.time_elapsed.toFixed(2)}}s /
         {{timer.duration().toFixed(2)}}s)
      </span>
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
             v-on:click="transport.duplicateStagedMessage(message)">
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

Vue.component('frankenpaxos-partition', {
  props: ['transport', 'address'],

  computed: {
    partitionedActors: function() {
      return this.JsUtils.setToJs(this.transport.partitionedActors);
    },
  },

  methods: {
    partition: function() {
      this.transport.partitionActor(this.address);
      this.$emit('partition', this.address);
    }
  },

  template: `
    <button class="frankenpaxos-button"
       v-bind:disabled="partitionedActors.includes(address)"
       v-on:click="partition">Partition</button>
  `,
});

Vue.component('frankenpaxos-unpartition', {
  props: ['transport', 'address'],

  computed: {
    partitionedActors: function() {
      return this.JsUtils.setToJs(this.transport.partitionedActors);
    },
  },

  methods: {
    unpartition: function() {
      this.transport.unpartitionActor(this.address);
      this.$emit('unpartition', this.address);
    }
  },

  template: `
    <button class="frankenpaxos-button"
       v-bind:disabled="!partitionedActors.includes(address)"
       v-on:click="unpartition">Unpartition</button>
  `,
});

Vue.component('frankenpaxos-unittest', {
  props: ['transport'],
  methods: {
    copy: function() {
      // https://hackernoon.com/copying-text-to-clipboard-with-javascript-df4d4988697f
      let content = this.JsUtils.seqToJs(this.transport.unitTest()).join('\n');
      let el = document.createElement('textarea');
      el.value = content;
      document.body.appendChild(el);
      el.select();
      document.execCommand('copy');
      document.body.removeChild(el);
    }
  },
  template: `
    <details>
      <summary>Unit test</summary>
      <button class="frankenpaxos-button"
               v-on:click="copy">Copy to clipboard</button>
      <div class="frankenpaxos-unittest">
        <div v-for="line in JsUtils.seqToJs(transport.unitTest())">
          <span class="frankenpaxos-unittest-line">
            {{line}}
          </span>
        </div>
      </div>
    </details>
  `
});

Vue.component('frankenpaxos-text-checkbox', {
  props: {
    text: String,
    value: Boolean,
  },

  template: `
    <div>
      <span
        v-on:click="$emit('input', !value)"
        :style="{cursor: 'pointer'}">
        {{text}}
      </span>
      <input
        type="checkbox"
        :checked="value"
        v-on:input="$emit('input', $event.target.checked)"
        :style="{cursor: 'pointer'}">
    </div>
  `
});

Vue.component('frankenpaxos-settings', {
  props: {
    transport: Object,
    value: Object,
  },

  methods: {
    emit: function(field, value) {
      let copy = Object.assign({}, this.value);
      copy[field] = value;
      this.$emit('input', copy);
    },
  },

  template: `
    <div class="frankenpaxos-box">
      <div>
        Time scale:
        <input
          type="range"
          min=0
          max=2
          step=0.1
          value="value.time_scale"
          v-on:input="emit('time_scale', parseFloat($event.target.value))">
        {{value.time_scale}}x
      </div>
      <frankenpaxos-text-checkbox
        :text="'Automatically deliver messages:'"
        :value="value.auto_deliver_messages"
        v-on:input="emit('auto_deliver_messages', $event)">
      </frankenpaxos-text-checkbox>
      <frankenpaxos-text-checkbox
        :text="'Automatically start timers:'"
        :value="value.auto_start_timers"
        v-on:input="emit('auto_start_timers', $event)">
      </frankenpaxos-text-checkbox>
      <frankenpaxos-text-checkbox
        :text="'Record history for unit tests:'"
        :value="value.record_history_for_unit_tests"
        v-on:input="transport.recordHistory = $event">
      </frankenpaxos-text-checkbox>
    </div>
  `
});

Vue.component('frankenpaxos-actor', {
  props: [
    'transport',
    'node',
    // node.actor
    // node.component
    'timer_tweens',
    // v-on:partition
    // v-on:unpartition
  ],

  methods: {
    timer_tweens_for: function(address) {
      return Object.values(this.timer_tweens).filter(
        tween => tween.data.timer.address === address);
    },
  },

  template: `
    <div class="frankenpaxos-node">
      <h2 :style="{ display: 'inline-block' }">{{node.actor.address.address}}</h2>
      <frankenpaxos-partition
        :transport=transport
        :address=node.actor.address
        v-on:partition="$emit('partition', $event)">
        partition
      </frankenpaxos-partition>
      <frankenpaxos-unpartition
        :transport=transport
        :address=node.actor.address
        v-on:unpartition="$emit('unpartition', $event)">
        unpartition
      </frankenpaxos-unpartition>

      <div class="frankenpaxos-box">
        <h3 class="frankenpaxos-box-title">Messages</h3>
        <frankenpaxos-staged-messages
          :transport="transport"
          :actor="node.actor"
          :messages="JsUtils.seqToJs(
            transport.stagedMessagesForAddress(node.actor.address))">
        </frankenpaxos-staged-messages>
      </div>

      <div class="frankenpaxos-box">
        <h3 class="frankenpaxos-box-title">Timers</h3>
        <div v-for="timer in timer_tweens_for(node.actor.address)">
          <frankenpaxos-timer :timer="timer"></frankenpaxos-timer>
        </div>
      </div>

      <div class="frankenpaxos-box">
        <h3 class="frankenpaxos-box-title">Data</h3>
        <keep-alive>
          <component :is="node.component" :node="node"></component>
        </keep-alive>
      </div>

      <div class="frankenpaxos-box">
        <h3 class="frankenpaxos-box-title">Log</h3>
        <frankenpaxos-log :log="JsUtils.seqToJs(node.actor.logger.log)">
        </frankenpaxos-log>
      </div>
    </div>
  `
});

// Data structure visualizations. //////////////////////////////////////////////
Vue.component('fp-toggle', {
  props: {
    value: {
      required: true,
    },
    show_at_start: {
      type: Boolean,
      default: true,
    },
  },

  data: function() {
    return {
      show: this.show_at_start,
    };
  },

  computed: {
    buttonName: function() {
      if (this.show) {
        return 'Hide';
      } else {
        return 'Show';
      }
    },
  },

  // TODO(mwhittaker): Add the persist thing.
  template: `
    <div>
      <button v-on:click="show = !show">{{buttonName}}</button>
      <slot v-if="show">{{value}}</slot>
      <span v-else>...</span>
    </div>
  `
});

Vue.component('frankenpaxos-map', {
  props: {
    map: {
      required: true,
    },
    show_values_at_start: {
      type: Boolean,
      default: true,
    },
  },

  data: function() {
    return {
      js_map: typeof this.map !== 'undefined' ?
                frankenpaxos.JsUtils.mapToJs(this.map) :
                {},
    };
  },

  watch: {
    map: {
      handler: function(map) {
        this.js_map = typeof map !== 'undefined' ?
                        frankenpaxos.JsUtils.mapToJs(map) :
                        {};
      },
      deep: true,
    }
  },

  template: `
    <table class="frankenpaxos-map">
      <tr v-for="kv in js_map">
        <td>{{kv[0]}}</td>
        <td>
          <fp-toggle :value="kv[1]" :show_at_start="show_values_at_start">
            <slot :value="kv[1]">{{kv[1]}}</slot>
          </fp-toggle>
        </td>
      </tr>
    </table>
  `
});

Vue.component('frankenpaxos-seq', {
  props: ['seq'],

  data: function() {
    return {
      js_seq: typeof this.seq !== 'undefined' ?
                frankenpaxos.JsUtils.seqToJs(this.seq):
                {},
    };
  },

  watch: {
    seq: {
      handler: function(seq) {
        this.js_seq = typeof seq !== 'undefined' ?
                        frankenpaxos.JsUtils.seqToJs(seq) :
                        {};
      },
      deep: true,
    }
  },

  template: `
    <table class="frankenpaxos-seq">
      <tr v-for="x in js_seq">
        <td><slot :value="x">{{x}}</slot></td>
      </tr>
    </table>
  `
});

Vue.component('frankenpaxos-set', {
  props: ['set'],

  data: function() {
    return {
      js_set: typeof this.set !== 'undefined' ?
                frankenpaxos.JsUtils.setToJs(this.set):
                {},
    };
  },

  watch: {
    set: {
      handler: function(set) {
        this.js_set = typeof set !== 'undefined' ?
                        frankenpaxos.JsUtils.setToJs(set) :
                        {};
      },
      deep: true,
    }
  },

  template: `
    <table class="frankenpaxos-set">
      <tr v-for="x in js_set">
        <td><slot :value="x">{{x}}</slot></td>
      </tr>
    </table>
  `
});

Vue.component('frankenpaxos-tuple', {
  props: ['tuple'],

  data: function() {
    return {
      js_tuple: typeof this.tuple !== 'undefined' ?
                  frankenpaxos.JsUtils.tupleToJs(this.tuple):
                  {},
    };
  },

  watch: {
    tuple: {
      handler: function(tuple) {
        this.js_tuple = typeof tuple !== 'undefined' ?
                          frankenpaxos.JsUtils.tupleToJs(tuple) :
                          {};
      },
      deep: true,
    }
  },

  template: `
    <table class="frankenpaxos-tuple">
      <tr v-for="x in js_tuple">
        <td><slot :value="x">{{x}}</slot></td>
      </tr>
    </table>
  `
});

Vue.component('fp-object', {
  props: {
    value: {
      default: null,
    }
  },

  template: `
    <div>
      <table class="fp-object">
        <slot :let="value">{{value}}</slot>
      </table>
    </div>
  `
});

Vue.component('fp-field', {
  props: {
    name: String,
    value: {
      default: null,
    },
    show_at_start: {default: true},
  },

  template: `
    <tr>
      <td>{{name}}</td>
      <td>
        <fp-toggle :value="value" :show_at_start="show_at_start">
          <slot :let="value">{{value}}</slot>
        </fp-toggle>
      </td>
    </tr>
  `
});

// Client table.
Vue.component('frankenpaxos-client-table', {
  props: {
    clientTable: Object,
  },

  template: `
    <frankenpaxos-map :map="clientTable.table" v-slot="{value: clientState}">
      <fp-object :value="clientState">
        <fp-field :name="'largestId'" :value="clientState.largestId">
        </fp-field>
        <fp-field :name="'largestOutput'" :value="clientState.largestOutput">
        </fp-field>
        <fp-field :name="'executedIds'" :value="clientState.executedIds">
        </fp-field>
      </fp-object>
    </frankenpaxos-map>
  `
});

// This is taken directly from https://github.com/alexcode/vue2vis.
const arrayDiff = (arr1, arr2) => arr1.filter(x => arr2.indexOf(x) === -1);

const mountVisData = (vm, propName) => {
  let data = vm[propName];
  // If data is DataSet or DataView we return early without attaching our own events
  if (!(vm[propName] instanceof vis.DataSet ||
        vm[propName] instanceof vis.DataView)) {
    data = new vis.DataSet(vm[propName]);
    // Rethrow all events
    data.on('*', (event, properties, senderId) =>
      vm.$emit(`${propName}-${event}`, { event, properties, senderId }));
    // We attach deep watcher on the prop to propagate changes in the DataSet
    const callback = (value) => {
      if (Array.isArray(value)) {
        const newIds = new vis.DataSet(value).getIds();
        const diff = arrayDiff(vm.visData[propName].getIds(), newIds);
        vm.visData[propName].update(value);
        vm.visData[propName].remove(diff);
      }
    };

    vm.$watch(propName, callback, {
      deep: true,
    });
  }

  // Emitting DataSets back
  vm.$emit(`${propName}-mounted`, data);

  return data;
};

Vue.component('frankenpaxos-graph', {
  props: [
    'edges',
    'nodes',
    'options',
  ],
  data: () => ({
    visData: {
      nodes: null,
      edges: null
    }
  }),
  watch: {
    options: {
      deep: true,
      handler(o) {
        this.network.setOptions(o);
      }
    }
  },
  methods: {
    setData(n, e) {
      this.visData.nodes = Array.isArray(n) ? new vis.DataSet(n) : n;
      this.visData.edges =  Array.isArray(e) ? new vis.DataSet(e) : e;
      this.network.setData(this.visData);
    },
    destroy() {
      this.network.destroy();
    },
  },
  created() {
    // This should be a Vue data property, but Vue reactivity kinda bugs Vis.
    // See here for more: https://github.com/almende/vis/issues/2524
    this.network = null;
  },
  mounted() {
    const container = this.$refs.visualization;
    this.visData.nodes = mountVisData(this, 'nodes');
    this.visData.edges = mountVisData(this, 'edges');
    this.network = new vis.Network(container, this.visData, this.options);
  },
  beforeDestroy() {
    this.network.destroy();
  },
  template: `
    <div ref="visualization"></div>
  `,
});
