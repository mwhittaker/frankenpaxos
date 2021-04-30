// Helper components ///////////////////////////////////////////////////////////
const phase1b_slot_info = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'slot'">
        {{value.slot}}
      </fp-field>
      <fp-field :name="'voteRound'">
        {{value.voteRound}}
      </fp-field>
      <fp-field :name="'voteValue'">
        {{value.voteValue}}
      </fp-field>
    </fp-object>
  `,
};

const phase1b_component = {
  props: {
    value: Object,
  },

  components: {
    'phase1b-slot-info': phase1b_slot_info,
  },

  template: `
    <fp-object>
      <fp-field :name="'acceptorIndex'">
        {{value.acceptorIndex}}
      </fp-field>
      <fp-field :name="'round'">
        {{value.round}}
      </fp-field>
      <fp-field :name="'info'">
        <frankenpaxos-seq :seq="value.info" v-slot="{value: info}">
          <phase1b-slot-info :value="inf">
          </phase1b-slot-info>
        </frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
};

const phase2b_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'acceptorIndex'">
        {{value.acceptorIndex}}
      </fp-field>
      <fp-field :name="'slot'">
        {{value.slot}}
      </fp-field>
      <fp-field :name="'round'">
        {{value.round}}
      </fp-field>
    </fp-object>
  `,
};

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
          PendingWrite
          <fp-object>
            <fp-field :name="'id'">{{state.id}}</fp-field>
            <fp-field :name="'command'">{{state.command}}</fp-field>
            <fp-field :name="'result'">{{state.result}}</fp-field>
            <fp-field :name="'resendClientRequest'">
              {{state.resendClientRequest}}
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <div>
        <button v-on:click="write">Write</button>
        <button v-on:click="write_ten">Write Ten</button>
        <input v-model="write_value" v-on:keyup.enter="write"></input>
      </div>
    </div>
  `,
};

const server_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        shardIndex = {{node.actor.shardIndex}}
      </div>

      <div>
        index = {{node.actor.index}}
      </div>

      <div>
        globalIndex = {{node.actor.globalIndex}}
      </div>

      <div>
        numServers = {{node.actor.numServers}}
      </div>

      <div>
        logs =
        <frankenpaxos-seq :seq="node.actor.logs" v-slot="{value: log}">
          <fp-object>
            <fp-field :name="'log'">
              <frankenpaxos-buffer-map :value="log.log">
              </frankenpaxos-buffer-map>
            </fp-field>
            <fp-field :name="'watermark'">
              {{log.watermark}}
            </fp-field>
            <fp-field :name="'numCommands'">
              {{log.numCommands}}
            </fp-field>
          </fp-object>
        </frankenpaxos-seq>
      </div>

      <div>
        cuts =
        <frankenpaxos-buffer-map :value="node.actor.cuts">
        </frankenpaxos-buffer-map>
      </div>
    </div>
  `,
};

const aggregator_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        shardCuts =
        <frankenpaxos-seq :seq="node.actor.shardCuts" v-slot="{value: shard}">
          <frankenpaxos-horizontal-seq :seq="shard">
          </frankenpaxos-horizontal-seq>
        </frankenpaxos-seq>
      </div>

      <div>
        numShardCutsSinceLastProposal =
        {{node.actor.numShardCutsSinceLastProposal}}
      </div>

      <div>
        rawCuts =
        <frankenpaxos-buffer-map :value="node.actor.rawCuts">
        </frankenpaxos-buffer-map>
      </div>

      <div>
        cuts =
        <frankenpaxos-horizontal-seq :seq="node.actor.cuts">
        </frankenpaxos-horizontal-seq>
      </div>

      <div>
        rawCutsWatermark = {{node.actor.rawCutsWatermark}}
      </div>

      <div>
        numRawCutsChosen = {{node.actor.numRawCutsChosen}}
      </div>
    </div>
  `,
}

const leader_info = {
  props: {
    node: Object,
  },

  components: {
    'phase1b': phase1b_component,
    'phase2b': phase2b_component,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        nextSlot = {{node.actor.nextSlot}}
      </div>

      <div>
        chosenWatermark = {{node.actor.chosenWatermark}}
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log">
        </frankenpaxos-buffer-map>
      </div>

      <div>
        state =
        <div v-if="node.actor.state.constructor.name.includes('Inactive')">
          Inactive
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase1')">
          Phase1
          <fp-object>
            <fp-field :name="'phase1bs'">
              <frankenpaxos-horizontal-map
                :map="node.actor.state.phase1bs"
                v-slot="{value: phase1b}">
                <phase1b :value="phase1b"></phase1b>
              </frankenpaxos-horizontal-map>
            </fp-field>
            <fp-field :name="'pendingProposals'">
              <frankenpaxos-seq :seq="node.actor.state.pendingProposals">
              </frankenpaxos-seq>
            </fp-field>
            <fp-field :name="'resendPhase1as'">
              {{node.actor.state.resendPhase1as}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase2')">
          Phase2
            <fp-field :name="'values'">
              <frankenpaxos-map :map="node.actor.state.values">
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'phase2bs'">
              <frankenpaxos-map :map="node.actor.state.phase2bs"
                                v-slot="{value: phase2bs}">
                phase2bs
                <frankenpaxos-horizontal-map :map="phase2bs"
                                             v-slot="{value: phase2b}">
                  <phase2b :value="phase2b"></phase2b>
                </frankenpaxos-horizontal-map>
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </div>
      </div>
    </div>
  `,
};

const election_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>
      <div>
        leaderIndex = {{node.actor.leaderIndex}}
      </div>
      <div>
        state = {{node.actor.state}}
      </div>
    </div>
  `,
};

const acceptor_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        states =
        <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
          <fp-object>
            <fp-field :name="'voteRound'">
              {{state.voteRound}}
            </fp-field>
            <fp-field :name="'voteValue'">
              {{state.voteValue}}
            </fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

const replica_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        clients =
        <frankenpaxos-map :map="node.actor.clients">
        </frankenpaxos-map>
      </div>

      <div>
        executedWatermark = {{node.actor.executedWatermark}}
      </div>

      <div>
        numChosen = {{node.actor.numChosen}}
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log">
        </frankenpaxos-buffer-map>
      </div>

      <div>
        clientTable =
        <frankenpaxos-map :map="node.actor.clientTable">
        </frankenpaxos-map>
      </div>
    </div>
  `,
};

// Main app ////////////////////////////////////////////////////////////////////
function distance(x1, y1, x2, y2) {
  const dx = x1 - x2;
  const dy = y1 - y2;
  return Math.sqrt(dx*dx + dy*dy);
}

function make_nodes(Scalog, snap, batched) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_yellow = '#f1c40f';
  const flat_orange = '#f39c12';
  const flat_green = '#2ecc71';
  const flat_purple = '#9b59b6';
  const flat_dark_blue = '#2c3e50';
  const flat_turquoise = '#1abc9c';

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
  const server_x = 200;
  const aggregator_x = 300;
  const leader_x = 400;
  const acceptor_x = 500;
  const replica_x = 600;

  const nodes = {};

  // Clients.
  const clients = [
    {client: Scalog.client1, y: 100},
    {client: Scalog.client2, y: 200},
    {client: Scalog.client3, y: 300},
    {client: Scalog.client4, y: 400},
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
    {server: Scalog.server1, y: 100},
    {server: Scalog.server2, y: 200},
    {server: Scalog.server3, y: 300},
    {server: Scalog.server4, y: 400},
  ]
  for (const [index, {server, y}] of servers.entries()) {
    const color = flat_blue;
    nodes[server.address] = {
      actor: server,
      color: color,
      component: server_info,
      svgs: [
        snap.circle(server_x, y, 20).attr(colored(color)),
        snap.text(server_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Aggregator.
  nodes[Scalog.aggregator.address] = {
    actor: Scalog.aggregator,
    color: flat_green,
    component: aggregator_info,
    svgs: [
      snap.circle(aggregator_x, 250, 20).attr(colored(flat_green)),
    ],
  };

  // Leaders.
  const leaders = [
    {leader: Scalog.leader1, y: 200, ey: 150},
    {leader: Scalog.leader2, y: 300, ey: 350},
  ]
  for (const [index, {leader, y, ey}] of leaders.entries()) {
    const color = flat_orange;
    nodes[leader.address] = {
      actor: leader,
      color: color,
      component: leader_info,
      svgs: [
        snap.circle(leader_x, y, 20).attr(colored(color)),
        snap.text(leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
    nodes[leader.electionAddress] = {
      actor: leader.election,
      color: color,
      component: election_info,
      svgs: [
        snap.circle(leader_x + 50, ey, 15).attr(colored(color)),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: Scalog.acceptor1, y: 150},
    {acceptor: Scalog.acceptor2, y: 250},
    {acceptor: Scalog.acceptor3, y: 350},
  ]
  for (const [index, {acceptor, y}] of acceptors.entries()) {
    const color = flat_purple;
    nodes[acceptor.address] = {
      actor: acceptor,
      color: color,
      component: acceptor_info,
      svgs: [
        snap.circle(acceptor_x, y, 20).attr(colored(color)),
        snap.text(acceptor_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Replicas.
  const replicas = [
    {replica: Scalog.replica1, y: 200},
    {replica: Scalog.replica2, y: 300},
  ]
  for (const [index, {replica, y}] of replicas.entries()) {
    const color = flat_dark_blue;
    nodes[replica.address] = {
      actor: replica,
      color: color,
      component: replica_info,
      svgs: [
        snap.circle(replica_x, y, 20).attr(colored(color)),
        snap.text(replica_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(server_x, 50, 'Servers'));
  anchor_middle(snap.text(aggregator_x, 50, 'Aggregator'));
  anchor_middle(snap.text(leader_x, 50, 'Leaders'));
  anchor_middle(snap.text(acceptor_x, 50, 'Acceptors'));
  anchor_middle(snap.text(replica_x, 50, 'Replicas'));

  return nodes;
}

function main() {
  const Scalog = frankenpaxos.scalog.Scalog.Scalog;
  const snap = Snap('#animation');
  const nodes = make_nodes(Scalog, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Scalog.client1.address],
      transport: Scalog.transport,
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
        let speed = 400; // px per second.

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

window.onload = main
