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
        round =
        <frankenpaxos-horizontal-seq :seq="node.actor.round">
        </frankenpaxos-horizontal-seq>
      </div>

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
        round =
        <frankenpaxos-horizontal-seq :seq="node.actor.round">
        </frankenpaxos-horizontal-seq>
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
        groupIndex = {{node.actor.groupIndex}}
      </div>
      <div>
        index = {{node.actor.index}}
      </div>
      <div>
        round = {{node.actor.round}}
      </div>
      <div>
        nextSlot = {{node.actor.nextSlot}}
      </div>
      <div>
        highWatermark = {{node.actor.highWatermark}}
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
        <div v-if="state.constructor.name.includes('PendingPhase2aNoopRange')">
          PendingPhase2aNoopRange
          <fp-object>
            <fp-field :name="'phase2aNoopRange'">
              {{state.phase2aNoopRange}}
            </fp-field>
            <fp-field :name="'phase2bNoopRanges'">
              {{state.phase2bNoopRanges}}
            </fp-field>
          </fp-object>
        </div>

        <div v-else-if="state.constructor.name.includes('PendingPhase2a')">
          PendingPhase2a
          <fp-object>
            <fp-field :name="'phase2a'">
              {{state.phase2a}}
            </fp-field>
            <fp-field :name="'phase2bs'">
              {{state.phase2bs}}
            </fp-field>
          </fp-object>
        </div>

        <div v-else-if="state.constructor.name.includes('Done')">
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
        leaderGroupIndex = {{node.actor.leaderGroupIndex}}
      </div>
      <div>
        acceptorGroupIndex = {{node.actor.acceptorGroupIndex}}
      </div>
      <div>
        index = {{node.actor.index}}
      </div>
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
function make_nodes(Mencius, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_orange = '#f39c12';
  const flat_green = '#2ecc71';
  const flat_purple = '#9b59b6';
  const flat_dark_blue = '#2c3e50';
  const flat_turquoise = '#1abc9c';
  const client_color = flat_red;
  const batcher_color = flat_blue;
  const leader_color = flat_orange;
  const proxy_leader_color = flat_green;
  const acceptor_color = flat_purple;
  const replica_color = flat_dark_blue;
  const proxy_replica_color = flat_turquoise;

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
  const leader_x = 300;
  const proxy_leader_x = 600;
  const acceptor_x = 700;
  const replica_x = 1000;
  const proxy_replica_x = 1100;

  const nodes = {};

  // Clients.
  const clients = [
    {client: Mencius.client1, y: 100},
    {client: Mencius.client2, y: 200},
    {client: Mencius.client3, y: 300},
    {client: Mencius.client4, y: 400},
  ]
  for (const [index, {client, y}] of clients.entries()) {
    nodes[client.address] = {
      actor: client,
      color: client_color,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(client_color)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Batchers.
  const batchers = [
    {batcher: Mencius.batcher1, y: 200},
    {batcher: Mencius.batcher2, y: 300},
  ]
  for (const [index, {batcher, y}] of batchers.entries()) {
    nodes[batcher.address] = {
      actor: batcher,
      color: batcher_color,
      component: batcher_info,
      svgs: [
        snap.circle(batcher_x, y, 20).attr(colored(batcher_color)),
        snap.text(batcher_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Leaders.
  const leaders = [
    {leader: Mencius.leaderA1, name: 'A1', x: leader_x,       y: 100, ey: 150},
    {leader: Mencius.leaderA2, name: 'A2', x: leader_x + 100, y: 100, ey: 150},
    {leader: Mencius.leaderA3, name: 'A3', x: leader_x + 200, y: 100, ey: 150},
    {leader: Mencius.leaderB1, name: 'B1', x: leader_x,       y: 400, ey: 350},
    {leader: Mencius.leaderB2, name: 'B2', x: leader_x + 100, y: 400, ey: 350},
    {leader: Mencius.leaderB3, name: 'B3', x: leader_x + 200, y: 400, ey: 350},
  ]
  for (const [index, {leader, name, x, y, ey}] of leaders.entries()) {
    nodes[leader.address] = {
      actor: leader,
      color: leader_color,
      component: leader_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(leader_color)),
        snap.text(x, y, name).attr(number_style),
      ],
    };
    nodes[leader.electionAddress] = {
      actor: leader.election,
      color: leader_color,
      component: election_info,
      svgs: [
        snap.circle(x + 50, ey, 15).attr(colored(leader_color)),
      ],
    };
  }

  // ProxyLeaders.
  const proxy_leaders = [
    {proxy_leader: Mencius.proxyLeader1, y: 200},
    {proxy_leader: Mencius.proxyLeader2, y: 300},
  ]
  for (const [index, {proxy_leader, y}] of proxy_leaders.entries()) {
    nodes[proxy_leader.address] = {
      actor: proxy_leader,
      color: proxy_leader_color,
      component: proxy_leader_info,
      svgs: [
        snap.circle(proxy_leader_x, y, 20).attr(colored(proxy_leader_color)),
        snap.text(proxy_leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acc: Mencius.acceptorA1, name: 'A1', x: acceptor_x,       y: 100},
    {acc: Mencius.acceptorA2, name: 'A2', x: acceptor_x + 100, y: 100},
    {acc: Mencius.acceptorA3, name: 'A3', x: acceptor_x + 200, y: 100},
    {acc: Mencius.acceptorB1, name: 'B1', x: acceptor_x,       y: 200},
    {acc: Mencius.acceptorB2, name: 'B2', x: acceptor_x + 100, y: 200},
    {acc: Mencius.acceptorB3, name: 'B3', x: acceptor_x + 200, y: 200},
    {acc: Mencius.acceptorC1, name: 'C1', x: acceptor_x,       y: 400},
    {acc: Mencius.acceptorC2, name: 'C2', x: acceptor_x + 100, y: 400},
    {acc: Mencius.acceptorC3, name: 'C3', x: acceptor_x + 200, y: 400},
  ]
  for (const [index, {acc, name, x, y}] of acceptors.entries()) {
    nodes[acc.address] = {
      actor: acc,
      color: acceptor_color,
      component: acceptor_info,
      svgs: [
        snap.circle(x, y, 20).attr(colored(acceptor_color)),
        snap.text(x, y, name).attr(number_style),
      ],
    };
  }

  // Replicas.
  const replicas = [
    {replica: Mencius.replica1, y: 200},
    {replica: Mencius.replica2, y: 300},
  ]
  for (const [index, {replica, y}] of replicas.entries()) {
    nodes[replica.address] = {
      actor: replica,
      color: replica_color,
      component: replica_info,
      svgs: [
        snap.circle(replica_x, y, 20).attr(colored(replica_color)),
        snap.text(replica_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // ProxyReplicas.
  const proxy_replicas = [
    {proxy_replica: Mencius.proxyReplica1, y: 200},
    {proxy_replica: Mencius.proxyReplica2, y: 300},
  ]
  for (const [index, {proxy_replica, y}] of proxy_replicas.entries()) {
    nodes[proxy_replica.address] = {
      actor: proxy_replica,
      color: proxy_replica_color,
      component: proxy_replica_info,
      svgs: [
        snap.circle(proxy_replica_x, y, 20).attr(colored(proxy_replica_color)),
        snap.text(proxy_replica_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(batcher_x, 50, 'Batchers'));
  anchor_middle(snap.text(leader_x + 100, 50, 'Leaders'));
  anchor_middle(snap.text(proxy_leader_x, 50, 'Proxy Leaders'));
  anchor_middle(snap.text(acceptor_x + 100, 50, 'Acceptors'));
  anchor_middle(snap.text(replica_x, 50, 'Replicas'));
  anchor_middle(snap.text(proxy_replica_x, 50, 'Proxy Replicas'));

  return nodes;
}

function main() {
  const Mencius = frankenpaxos.mencius.Mencius.Mencius;
  const snap = Snap('#animation');
  const nodes = make_nodes(Mencius, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Mencius.client1.address],
      transport: Mencius.transport,
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
