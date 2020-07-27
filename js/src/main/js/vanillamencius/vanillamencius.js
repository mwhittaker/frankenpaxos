// Node components /////////////////////////////////////////////////////////////
const client_info = {
  props: {
    node: Object,
  },

  data: function() {
    return {
      write_value: "",
    };
  },

  methods: {
    write: function() {
      if (this.write_value === "") {
        return;
      }
      this.node.actor.write(0, this.write_value);
      this.write_value = "";
    },

    write_ten: function() {
      if (this.write_value === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.write(i, this.write_value);
      }
      this.write_value = "";
    },
  },

  template: `
    <div>
      <div>
        ids = <frankenpaxos-map :map="node.actor.ids"></frankenpaxos-map>
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <div v-if="state.constructor.name.includes('PendingWrite')">
            PendingWrite
            <fp-object>
              <fp-field :name="'pseudonym'">{{state.pseudonym}}</fp-field>
              <fp-field :name="'id'">{{state.id}}</fp-field>
              <fp-field :name="'command'">{{state.command}}</fp-field>
              <fp-field :name="'result'">{{state.result}}</fp-field>
              <fp-field :name="'resendClientRequest'">
                {{state.resendClientRequest}}
              </fp-field>
            </fp-object>
          </div>
        </frankenpaxos-map>
      </div>

      <div>
        <button v-on:click="write">Write</button>
        <button v-on:click="write_ten">Write Ten</button>
        <input v-model="write_value" v-on:keyup.enter="write"></input>
      </div>
    </div>
  `,
}

const server_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        nextSlot = {{node.actor.nextSlot}}
      </div>

      <div>
        executedWatermark = {{node.actor.executedWatermark}}
      </div>

      <div>
        skipSlots = {{node.actor.skipSlots}}
      </div>

      <div>
        recoverRound = {{node.actor.recoverRound}}
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log"
                                 v-slot="{value: entry}">
          <frankenpaxos-option :value="entry">
            <template v-slot:if="{value: entry}">
              <div v-if="entry.constructor.name.includes('VotelessEntry')">
                VotelessEntry
                <fp-object>
                  <fp-field :name="'round'">{{entry.round}}</fp-field>
                </fp-object>
              </div>

              <div v-if="entry.constructor.name.includes('PendingEntry')">
                PendingEntry
                <fp-object>
                  <fp-field :name="'round'">{{entry.round}}</fp-field>
                  <fp-field :name="'voteRound'">{{entry.voteRound}}</fp-field>
                  <fp-field :name="'voteValue'">{{entry.voteValue}}</fp-field>
                </fp-object>
              </div>

              <div v-if="entry.constructor.name.includes('ChosenEntry')">
                ChosenEntry
                <fp-object>
                  <fp-field :name="'value'">{{entry.value}}</fp-field>
                </fp-object>
              </div>
            </template>
            <template v-slot:else="_">
              None
            </template>
          </frankenpaxos-option>
        </frankenpaxos-buffer-map>
      </div>

      <div>
        stateMachine =
        {{node.actor.stateMachine}}
      </div>

      <div>
        phase1s =
        <frankenpaxos-map :map="node.actor.phase1s" v-slot="{value: phase1}">
          <fp-object>
            <fp-field :name="'startSlotInclusive'">
              {{phase1.startSlotInclusive}}
            </fp-field>
            <fp-field :name="'stopSlotExclusive'">
              {{phase1.stopSlotExclusive}}
            </fp-field>
            <fp-field :name="'round'">{{phase1.round}}</fp-field>
            <fp-field :name="'phase1bs'">
              <frankenpaxos-map :map="phase1.phase1bs">
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <div>
        phase2s =
        <frankenpaxos-map :map="node.actor.phase2s" v-slot="{value: phase2}">
          <fp-object>
            <fp-field :name="'round'">
              {{phase2.round}}
            </fp-field>
            <fp-field :name="'value'">
              {{phase2.value}}
            </fp-field>
            <fp-field :name="'phase2bs'">
              <frankenpaxos-map :map="phase2.phase2bs">
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <div>
        largestChosenPrefixSlots =
        <frankenpaxos-horizontal-seq :seq="node.actor.largestChosenPrefixSlots">
        </frankenpaxos-horizontal-seq>
      </div>

      <div>
        clientTable =
        <frankenpaxos-map :map="node.actor.clientTable">
        </frankenpaxos-map>
      </div>
    </div>
  `,
}

const heartbeat_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        addresses =
        <frankenpaxos-seq :seq="node.actor.addresses"></frankenpaxos-seq>
      </div>

      <div>
        numRetries =
        <frankenpaxos-seq :seq="node.actor.numRetries"></frankenpaxos-seq>
      </div>

      <div>
        networkDelayNanos =
        <frankenpaxos-map :map="node.actor.networkDelayNanos">
        </frankenpaxos-map>
      </div>

      <div>
        alive =
        <frankenpaxos-set :set="node.actor.alive"></frankenpaxos-set>
      </div>
    </div>
  `,
};

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(VanillaMencius, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';

  const colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  const number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  const client_x = 100;
  const nodes = {};

  // Clients.
  const clients = [
    {client: VanillaMencius.client1, y: 100},
    {client: VanillaMencius.client2, y: 200},
    {client: VanillaMencius.client3, y: 300},
    {client: VanillaMencius.client4, y: 400},
  ]
  for (const [index, {client, y}] of clients.entries()) {
    const color = flat_red;
    nodes[client.address] = {
      actor: client,
      color: color,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(color)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Servers.
  const servers = [
    {server: VanillaMencius.server1, x: 300, y: 150, hx: 350, hy: 150},
    {server: VanillaMencius.server2, x: 500, y: 250, hx: 450, hy: 250},
    {server: VanillaMencius.server3, x: 300, y: 350, hx: 350, hy: 350},
  ];
  for (const [index, {server, x, y, hx, hy}] of servers.entries()) {
    const color = flat_blue;
    nodes[server.address] = {
      actor: server,
      color: color,
      component: server_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(color)),
        snap.text(x, y, (index + 1).toString()).attr(number_style),
      ],
    };
    nodes[server.heartbeatAddress] = {
      actor: server.heartbeat,
      color: color,
      component: heartbeat_info,
      svgs: [
        snap.circle(hx, hy, 15).attr(colored(color)),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(400, 50, 'Servers'));

  return nodes;
}

function distance (x1, y1, x2, y2) {
  const dx = x1 - x2;
  const dy = y1 - y2;
  return Math.sqrt(dx*dx + dy*dy);
}

function make_app(f, VanillaMencius, app_id, snap_id) {
}

function main() {
  // make_app(frankenpaxos.vanillamencius.VanillaMencius, '#app', '#animation');
  const VanillaMencius =
    frankenpaxos.vanillamencius.VanillaMencius.VanillaMencius;
  const snap = Snap('#animation');
  const nodes = make_nodes(VanillaMencius, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[VanillaMencius.client1.address],
      transport: VanillaMencius.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = distance(src_x, src_y, dst_x, dst_y);
        let speed = 400 + (Math.random() * 50); // px per second.

        let svg_message = src.actor.constructor.name.includes("heartbeat") ?
          snap.circle(src_x, src_y, 6).attr({fill: '#bdc3c7'}) :
          snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        // If a node sends a message to itself, the duration is 0 and the tween
        // library doesn't behave properly. We make sure to always have a
        // positive duration.
        let duration = Math.max(0.001, d / speed);
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        nodes[address].svgs[0].attr({fill: "#7f8c8d"})
      },

      unpartition: function(address) {
        nodes[address].svgs[0].attr({fill: nodes[address].color})
      },
    },
  });

  // Select a node by clicking it.
  for (const node of Object.values(nodes)) {
    for (const svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

window.onload = main
