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
        ids =
        <frankenpaxos-map :map="node.actor.ids"></frankenpaxos-map>
      </div>

      <div>
        pendingCommands =
        <frankenpaxos-map
          :map="node.actor.pendingCommands"
          v-slot="{value: pc}">
          <fp-object>
            <fp-field :name="'pseudonym'" :value="pc.pseudonym"></fp-field>
            <fp-field :name="'id'" :value="pc.id"></fp-field>
            <fp-field :name="'command'" :value="pc.command"></fp-field>
            <fp-field :name="'result'" :value="pc.result"></fp-field>
          </fp-object>
        </frankenpaxos-map>
      </div>

      <button v-on:click="propose">Propose</button>
      <button v-on:click="propose_ten">Propose Ten</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>

    </div>
  `,
}

const batcher_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        round = {{node.actor.round}}
      </div>

      <div>
        growingBatch =
        <frankenpaxos-horizontal-seq :seq="node.actor.growingBatch">
        </frankenpaxos-horizontal-seq>
      </div>

      <div>
        pendingResendBatches =
        <frankenpaxos-horizontal-seq :seq="node.actor.pendingResendBatches">
        </frankenpaxos-horizontal-seq>
      </div>
    </div>
  `,
}

const proposer_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
    </div>
  `,
}

const disseminator_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
    </div>
  `,
}

const leader_info = {
  props: {
    node: Object,
  },

  components: {
    'phase1b': phase1b_component,
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
        state =
        <div v-if="node.actor.state.constructor.name.includes('Inactive')">
          Inactive
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase1')">
          Phase1
          <fp-object>
            <fp-field :name="'phase1bs'">
              <frankenpaxos-seq
                :seq="node.actor.state.phase1bs"
                v-slot="{value: phase1bs}">
                <frankenpaxos-horizontal-map
                  :map="phase1bs"
                  v-slot="{value: phase1b}">
                  <phase1b :value="phase1b"></phase1b>
                </frankenpaxos-horizontal-map>
              </frankenpaxos-seq>
            </fp-field>
            <fp-field :name="'pendingClientRequestBatches'">
              <frankenpaxos-seq :seq="node.actor.state.pendingClientRequestBatches">
              </frankenpaxos-seq>
            </fp-field>
            <fp-field :name="'resendPhase1as'">
              {{node.actor.state.resendPhase1as}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase2')">
          Phase2
        </div>
      </div>
    </div>
  `,
}

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

const proxy_leader_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      states =
      <frankenpaxos-map :map="node.actor.states" v-slot="{value: state}">
        <div v-if="state.constructor.name.includes('Pending')">
          <fp-object>
            <fp-field :name="'phase2a'">
              {{state.phase2a}}
            </fp-field>
            <fp-field :name="'phase2bs'">
              {{state.phase2bs}}
            </fp-field>
          </fp-object>
        </div>

        <div v-if="state.constructor.name.includes('Done')">
          Done
        </div>
      </frankenpaxos-map>
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
}

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
}

const proxy_replica_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
    </div>
  `,
}

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(SPaxosDecouple, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_orange = '#f39c12';
  const flat_green = '#2ecc71';
  const flat_purple = '#9b59b6';
  const flat_dark_blue = '#2c3e50';
  const flat_turquoise = '#1abc9c';
  const flat_gray = '#808080';

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
  const batcher_x = 200;
  const proposer_x = 300;
  const leader_x = 400;
  const proxy_leader_x = 700;
  const replica_x = 900;
  const proxy_replica_x = 1000;

  const nodes = {};

  // Clients.
  const clients = [
    {client: SPaxosDecouple.client1, y: 100},
    {client: SPaxosDecouple.client2, y: 200},
    {client: SPaxosDecouple.client3, y: 300},
    {client: SPaxosDecouple.client4, y: 400},
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

  // Batchers.
  const batchers = [
    {batcher: SPaxosDecouple.batcher1, y: 200},
    {batcher: SPaxosDecouple.batcher2, y: 300},
  ]
  for (const [index, {batcher, y}] of batchers.entries()) {
    const color = flat_blue;
    nodes[batcher.address] = {
      actor: batcher,
      color: color,
      component: batcher_info,
      svgs: [
        snap.circle(batcher_x, y, 20).attr(colored(color)),
        snap.text(batcher_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Proposers.
  const proposers = [
    {proposer: SPaxosDecouple.proposer1, y: 200},
    {proposer: SPaxosDecouple.proposer2, y: 300},
  ]
  for (const [index, {proposer, y}] of proposers.entries()) {
    const color = flat_blue;
    nodes[proposer.address] = {
      actor: proposer,
      color: color,
      component: proposer_info,
      svgs: [
        snap.circle(proposer_x, y, 20).attr(colored(color)),
        snap.text(proposer_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  const disseminators = [
    {acc: SPaxosDecouple.disseminatorD1, name: 'D1', x: proposer_x+ 100, y: 100},
    {acc: SPaxosDecouple.disseminatorD2, name: 'D2', x: proposer_x+ 200, y: 100},
    {acc: SPaxosDecouple.disseminatorD3, name: 'D3', x: proposer_x+ 300, y: 100},
    {acc: SPaxosDecouple.disseminatorE1, name: 'E1', x: proposer_x+ 100, y: 400},
    {acc: SPaxosDecouple.disseminatorE2, name: 'E2', x: proposer_x+ 200, y: 400},
    {acc: SPaxosDecouple.disseminatorE3, name: 'E3', x: proposer_x+ 300, y: 400},
  ]
  for (const [index, {acc, name, x, y}] of disseminators.entries()) {
    const color = flat_gray;
    nodes[acc.address] = {
      actor: acc,
      color: color,
      component: disseminator_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(color)),
        snap.text(x, y, name).attr(number_style),
      ],
    };
  }

  // Leaders.
  const leaders = [
    {leader: SPaxosDecouple.leader1, y: 200, ey: 150},
    {leader: SPaxosDecouple.leader2, y: 300, ey: 350},
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

  // ProxyLeaders.
  const proxy_leaders = [
    {proxy_leader: SPaxosDecouple.proxyLeader1, y: 200},
    {proxy_leader: SPaxosDecouple.proxyLeader2, y: 300},
  ]
  for (const [index, {proxy_leader, y}] of proxy_leaders.entries()) {
    const color = flat_green;
    nodes[proxy_leader.address] = {
      actor: proxy_leader,
      color: color,
      component: proxy_leader_info,
      svgs: [
        snap.circle(proxy_leader_x, y, 20).attr(colored(color)),
        snap.text(proxy_leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acc: SPaxosDecouple.acceptorA1, name: 'A1', x: proxy_leader_x+ 100, y: 100},
    {acc: SPaxosDecouple.acceptorA2, name: 'A2', x: proxy_leader_x+ 200, y: 100},
    {acc: SPaxosDecouple.acceptorA3, name: 'A3', x: proxy_leader_x+ 300, y: 100},
    {acc: SPaxosDecouple.acceptorB1, name: 'B1', x: proxy_leader_x+ 100, y: 400},
    {acc: SPaxosDecouple.acceptorB2, name: 'B2', x: proxy_leader_x+ 200, y: 400},
    {acc: SPaxosDecouple.acceptorB3, name: 'B3', x: proxy_leader_x+ 300, y: 400},
  ]
  for (const [index, {acc, name, x, y}] of acceptors.entries()) {
    const color = flat_purple;
    nodes[acc.address] = {
      actor: acc,
      color: color,
      component: acceptor_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(color)),
        snap.text(x, y, name).attr(number_style),
      ],
    };
  }

  // Replicas.
  const replicas = [
    {replica: SPaxosDecouple.replica1, y: 200},
    {replica: SPaxosDecouple.replica2, y: 300},
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

  // ProxyReplicas.
  const proxy_replicas = [
    {proxy_replica: SPaxosDecouple.proxyReplica1, y: 200},
    {proxy_replica: SPaxosDecouple.proxyReplica2, y: 300},
  ]
  for (const [index, {proxy_replica, y}] of proxy_replicas.entries()) {
    const color = flat_turquoise;
    nodes[proxy_replica.address] = {
      actor: proxy_replica,
      color: color,
      component: proxy_replica_info,
      svgs: [
        snap.circle(proxy_replica_x, y, 20).attr(colored(color)),
        snap.text(proxy_replica_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(batcher_x, 50, 'Batchers'));
  anchor_middle(snap.text(leader_x, 50, 'Leaders'));
  anchor_middle(snap.text(proxy_leader_x, 50, 'Proxy Leaders'));
  anchor_middle(snap.text(proxy_leader_x + 200, 50, 'Acceptors'));
  anchor_middle(snap.text(replica_x, 50, 'Replicas'));
  anchor_middle(snap.text(proxy_replica_x, 50, 'Proxy Replicas'));

  return nodes;
}

function main() {
  const SPaxosDecouple = frankenpaxos.spaxosdecouple.SPaxosDecouple.SPaxosDecouple;
  const snap = Snap('#animation');
  const nodes = make_nodes(SPaxosDecouple, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[SPaxosDecouple.client1.address],
      transport: SPaxosDecouple.transport,
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

window.onload = main
