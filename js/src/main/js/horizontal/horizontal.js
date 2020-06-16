// Helper components ///////////////////////////////////////////////////////////
const phase = {
  props: {
    value: Object,
  },

  template: `
    <div>
      <div v-if="value.constructor.name.includes('Phase1')">
        Phase 1
        <fp-object>
          <fp-field :name="'phase1bs'">
            {{value.phase1bs}}
          </fp-field>
          <fp-field :name="'resendPhase1as'">
            {{value.resendPhase1as}}
          </fp-field>
        </fp-object>
      </div>

      <div v-if="value.constructor.name.includes('Phase2')">
        Phase 2

        <fp-object>
          <fp-field :name="'nextSlot'">
            {{value.nextSlot}}
          </fp-field>
          <fp-field :name="'values'">
            {{value.values}}
          </fp-field>
          <fp-field :name="'phase2bs'">
            {{value.phase2bs}}
          </fp-field>
        </fp-object>
      </div>
    </div>
  `,
}

const chunk = {
  props: {
    value: Object,
  },

  components: {
    'phase': phase,
  },

  template: `
    <fp-object>
      <fp-field :name="'firstSlot'">
        {{value.firstSlot}}
      </fp-field>
      <fp-field :name="'lastSlot'">
        {{value.lastSlot}}
      </fp-field>
      <fp-field :name="'quorumSystem'">
        {{value.quorumSystem}}
      </fp-field>
      <fp-field :name="'phase'">
        <phase :value="value.phase"></phase>
      </fp-field>
    </fp-object>
  `,
}

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
}

const phase1b_component = {
  props: {
    value: Object,
  },

  components: {
    'phase1b-slot-info': phase1b_slot_info,
  },

  template: `
    <fp-object>
      <fp-field :name="'groupIndex'">
        {{value.groupIndex}}
      </fp-field>
      <fp-field :name="'acceptorIndex'">
        {{value.acceptorIndex}}
      </fp-field>
      <fp-field :name="'round'">
        {{value.round}}
      </fp-field>
      <fp-field :name="'round'">
        <frankenpaxos-seq :seq="value.info">
          <phase1b-slot-info :value="value.info">
          </phase1b-slot-info>
        </frankenpaxos-seq>
      </fp-field>
    </fp-object>
  `,
}

const phase1_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object :value="value" v-slot="{let: state}">
      <fp-field :name="'round'">{{state.round}}</fp-field>
      <fp-field :name="'quorumSystem'">{{state.quorumSystem}}</fp-field>
      <fp-field :name="'previousQuorumSystems'">
        <frankenpaxos-map :map="state.previousQuorumSystems">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'acceptorToRounds'">
        <frankenpaxos-map :map="state.acceptorToRounds">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'pendingRounds'">
        {{state.pendingRounds}}
      </fp-field>
      <fp-field :name="'phase1bs'">
        <frankenpaxos-map :map="state.phase1bs">
        </frankenpaxos-map>
      </fp-field>
      <fp-field :name="'pendingClientRequests'">
        <frankenpaxos-horizontal-seq :seq="state.pendingClientRequests">
        </frankenpaxos-horizontal-seq>
      </fp-field>
      <fp-field :name="'resendPhase1as'">
        {{state.resendPhase1as}}
      </fp-field>
    </fp-object>
  `
}

// Node components /////////////////////////////////////////////////////////////
const client_info = {
  props: {
    node: Object,
  },

  data: function() {
    return {
      proposal: "",
    };
  },

  methods: {
    propose: function() {
      if (this.proposal === "") {
        return;
      }
      this.node.actor.propose(0, this.proposal);
      this.proposal = "";
    },

    propose_ten: function() {
      if (this.proposal === "") {
        return;
      }
      for (let i = 0; i < 10; ++i) {
        this.node.actor.propose(i, this.proposal);
      }
      this.proposal = "";
    },
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        ids =
        <frankenpaxos-map :map="node.actor.ids">
        </frankenpaxos-map>
      </div>

      <div>
        pendingCommands =
        <frankenpaxos-map
          :map="node.actor.pendingCommands"
          v-slot="{value: pc}">
          <fp-object>
            <fp-field :name="'pseudonym'">{{pc.pseudonym}}</fp-field>
            <fp-field :name="'id'">{{pc.id}}</fp-field>
            <fp-field :name="'command'">{{pc.command}}</fp-field>
            <fp-field :name="'result'">{{pc.result}}</fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <button v-on:click="propose">Propose</button>
      <button v-on:click="propose_ten">Propose Ten</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

const leader_info = {
  props: {
    node: Object,
  },

  components: {
    'chunk': chunk,
  },

  template: `
    <div>
      <button v-on:click="node.actor.reconfigure()">Reconfigure</button>

      <div>
        chosenWatermark = {{node.actor.chosenWatermark}}
      </div>

      <div>
        activeFirstSlots =
        <frankenpaxos-horizontal-seq :seq="node.actor.activeFirstSlots">
        </frankenpaxos-horizontal-seq>
      </div>

      <div>
        log =
        <frankenpaxos-buffer-map :value="node.actor.log">
        </frankenpaxos-buffer-map>
      </div>

      <div>
        state =
        <div v-if="node.actor.state.constructor.name.endsWith('Inactive')">
          Inactive
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'round'">
              {{state.round}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.endsWith('Active')">
          Active
          <fp-object :value="node.actor.state" v-slot="{let: state}">
            <fp-field :name="'round'">
              {{state.round}}
            </fp-field>
            <fp-field :name="'chunks'">
              <frankenpaxos-seq :seq="state.chunks" v-slot="{value: chunk}">
                <chunk :value="chunk"></chunk>
              </frankenpaxos-seq>
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
}

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
            <fp-field :name="'firstSlot'">{{state.firstSlot}}</fp-field>
            <fp-field :name="'voteRound'">{{state.voteRound}}</fp-field>
            <fp-field :name="'voteValue'">{{state.voteValue}}</fp-field>
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
}

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(Horizontal, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
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
  const leader_x = 200;
  const acceptor_x = 300;
  const replica_x = 400;

  const nodes = {};

  // Clients.
  const clients = [
    {client: Horizontal.client1, y: 300},
    {client: Horizontal.client2, y: 400},
    {client: Horizontal.client3, y: 500},
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

  // Leaders.
  const leaders = [
    {leader: Horizontal.leader1, y: 350, ey: 325},
    {leader: Horizontal.leader2, y: 450, ey: 475},
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
        snap.circle(leader_x + 25, ey, 10).attr(colored(color)),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: Horizontal.acceptor1, y: 100},
    {acceptor: Horizontal.acceptor2, y: 200},
    {acceptor: Horizontal.acceptor3, y: 300},
    {acceptor: Horizontal.acceptor4, y: 400},
    {acceptor: Horizontal.acceptor5, y: 500},
    {acceptor: Horizontal.acceptor6, y: 600},
    {acceptor: Horizontal.acceptor7, y: 700},
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
    {replica: Horizontal.replica1, y: 300},
    {replica: Horizontal.replica2, y: 400},
    {replica: Horizontal.replica3, y: 500},
  ]
  for (const [index, {replica, y}] of replicas.entries()) {
    const color = flat_blue;
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
  anchor_middle(snap.text(leader_x, 50, 'Leaders'));
  anchor_middle(snap.text(acceptor_x, 50, 'Acceptors'));
  anchor_middle(snap.text(replica_x, 50, 'Replicas'));

  return nodes;
}

function main() {
  const Horizontal = frankenpaxos.horizontal.Horizontal.Horizontal;
  const snap = Snap('#animation');
  const nodes = make_nodes(Horizontal, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Horizontal.client1.address],
      transport: Horizontal.transport,
      settings: {
        time_scale: 1,
        auto_deliver_messages: true,
        auto_start_timers: true,
      },
    },

    methods: {
      distance: function(x1, y1, x2, y2) {
        const dx = x1 - x2;
        const dy = y1 - y2;
        return Math.sqrt(dx*dx + dy*dy);
      },

      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = this.distance(src_x, src_y, dst_x, dst_y);
        let speed = 200 + (Math.random() * 50); // px per second.

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
