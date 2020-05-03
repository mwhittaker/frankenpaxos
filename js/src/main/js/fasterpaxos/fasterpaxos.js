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
        round = {{node.actor.round}}
      </div>

      <div>
        delegates =
        <frankenpaxos-horizontal-seq :seq="node.actor.delegates">
        </frankenpaxos-horizontal-seq>
      </div>

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
        executedWatermark =
        {{node.actor.executedWatermark}}
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log"
                                 v-slot="{value: entry}">
          <frankenpaxos-option :value="entry">
            <template v-slot:if="{value: entry}">
              <div v-if="entry.constructor.name.includes('PendingEntry')">
                PendingEntry
                <fp-object>
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
        state =
        <div v-if="node.actor.state.constructor.name.includes('Phase1')">
          Phase1
          <fp-object>
            <fp-field :name="'round'">{{node.actor.state.round}}</fp-field>
            <fp-field :name="'delegates'">
              <frankenpaxos-horizontal-seq :seq="node.actor.state.delegates">
              </frankenpaxos-horizontal-seq>
            </fp-field>
            <fp-field :name="'phase1bs'">
              <frankenpaxos-map :map="node.actor.state.phase1bs">
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'pendingClientRequests'">
              <frankenpaxos-horizontal-seq
                :seq="node.actor.state.pendingClientRequests">
              </frankenpaxos-horizontal-seq>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase2')">
          Phase2
          <fp-object>
            <fp-field :name="'round'">{{node.actor.state.round}}</fp-field>
            <fp-field :name="'delegates'">
              <frankenpaxos-horizontal-seq :seq="node.actor.state.delegates">
              </frankenpaxos-horizontal-seq>
            </fp-field>
            <fp-field :name="'delegateIndex'">
              {{node.actor.state.delegateIndex}}
            </fp-field>
            <fp-field :name="'anyWatermark'">
              {{node.actor.state.anyWatermark}}
            </fp-field>
            <fp-field :name="'nextSlot'">
              {{node.actor.state.nextSlot}}
            </fp-field>
            <fp-field :name="'pendingValues'">
              <frankenpaxos-map :map="node.actor.state.pendingValues">
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'phase2bs'">
              <frankenpaxos-map :map="node.actor.state.phase2bs"
                                v-slot="{value: phase2bs}">
                <frankenpaxos-map :map="phase2bs">
                </frankenpaxos-map>
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'waitingPhase2aAnyAcks'">
              <frankenpaxos-set :set="node.actor.state.waitingPhase2aAnyAcks">
              </frankenpaxos-set>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Delegate')">
          Delegate
          <fp-object>
            <fp-field :name="'round'">{{node.actor.state.round}}</fp-field>
            <fp-field :name="'delegates'">
              <frankenpaxos-horizontal-seq :seq="node.actor.state.delegates">
              </frankenpaxos-horizontal-seq>
            </fp-field>
            <fp-field :name="'delegateIndex'">
              {{node.actor.state.delegateIndex}}
            </fp-field>
            <fp-field :name="'anyWatermark'">
              {{node.actor.state.anyWatermark}}
            </fp-field>
            <fp-field :name="'nextSlot'">
              {{node.actor.state.nextSlot}}
            </fp-field>
            <fp-field :name="'pendingValues'">
              <frankenpaxos-map :map="node.actor.state.pendingValues">
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'phase2bs'">
              <frankenpaxos-map :map="node.actor.state.phase2bs"
                                v-slot="{value: phase2bs}">
                <frankenpaxos-map :map="phase2bs">
                </frankenpaxos-map>
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Idle')">
          Idle
          <fp-object>
            <fp-field :name="'round'">{{node.actor.state.round}}</fp-field>
            <fp-field :name="'delegates'">
              <frankenpaxos-horizontal-seq :seq="node.actor.state.delegates">
              </frankenpaxos-horizontal-seq>
            </fp-field>
          </fp-object>
        </div>
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
function make_nodes(f, FasterPaxos, snap) {
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
    {client: FasterPaxos.FasterPaxos.client1, y: 100},
    {client: FasterPaxos.FasterPaxos.client2, y: 200},
    {client: FasterPaxos.FasterPaxos.client3, y: 300},
    {client: FasterPaxos.FasterPaxos.client4, y: 400},
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
  const servers = f == 1 ?
    [
      {server: FasterPaxos.server1, x: 300, y: 150, ex: 350, ey: 150},
      {server: FasterPaxos.server2, x: 500, y: 250, ex: 450, ey: 250},
      {server: FasterPaxos.server3, x: 300, y: 350, ex: 350, ey: 350},
    ]
    :
    [
      {server: FasterPaxos.server1, x: 250, y: 150, ex: 280, ey: 180},
      {server: FasterPaxos.server2, x: 400, y: 150, ex: 370, ey: 180},
      {server: FasterPaxos.server3, x: 550, y: 250, ex: 500, ey: 250},
      {server: FasterPaxos.server4, x: 400, y: 350, ex: 370, ey: 320},
      {server: FasterPaxos.server5, x: 250, y: 350, ex: 280, ey: 320},
    ];
  for (const [index, {server, x, y, ex, ey}] of servers.entries()) {
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
        snap.circle(ex, ey, 15).attr(colored(color)),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  if (f == 1) {
    anchor_middle(snap.text(400, 50, 'Servers'));
  } else {
    anchor_middle(snap.text(300, 50, 'Servers'));
  }

  return nodes;
}

function distance (x1, y1, x2, y2) {
  const dx = x1 - x2;
  const dy = y1 - y2;
  return Math.sqrt(dx*dx + dy*dy);
}

function make_app(f, FasterPaxos, app_id, snap_id) {
  const snap = Snap(snap_id);
  const nodes = make_nodes(f, FasterPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    data: {
      nodes: nodes,
      node: nodes[FasterPaxos.FasterPaxos.client1.address],
      transport: FasterPaxos.FasterPaxos.transport,
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

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = d / speed;
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

function main() {
  make_app(1, frankenpaxos.fasterpaxos.FasterPaxosF1,
           '#app_f1', '#animation_f1');
  make_app(2, frankenpaxos.fasterpaxos.FasterPaxosF2,
           '#app_f2', '#animation_f2');
}

window.onload = main
