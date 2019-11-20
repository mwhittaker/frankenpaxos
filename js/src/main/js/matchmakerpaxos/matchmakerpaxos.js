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
      this.node.actor.propose(this.proposal);
      this.proposal = "";
    },
  },

  template: `
    <div>
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

      <div>
        state =
        <div v-if="node.actor.state.constructor.name.includes('Inactive')">
          Inactive
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Pending')">
          Pending
          <fp-object>
            <fp-field
              :name="'promises'"
              :value="node.actor.state.promises">
            </fp-field>
            <fp-field
              :name="'resendClientRequest'"
              :value="node.actor.state.resendClientRequest">
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Chosen')">
          Chosen
          <fp-object>
            <fp-field :name="'v'" :value="node.actor.state.v">
            </fp-field>
          </fp-object>
        </div>
      </div>

      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>

    </div>
  `,
};

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
        state =
        <div v-if="node.actor.state.constructor.name.includes('Inactive')">
          Inactive
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Matchmaking')">
          Matchmaking
          <fp-object>
            <fp-field :name="'v'" :value="node.actor.state.v"></fp-field>
            <fp-field :name="'writeAcceptorGroup'"
                      :value="node.actor.state.writeAcceptorGroup">
            </fp-field>
            <fp-field :name="'matchReplies'">
              <frankenpaxos-map :map="node.actor.state.matchReplies">
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase1')">
          Phase1
          <fp-object>
            <fp-field :name="'v'" :value="node.actor.state.v"></fp-field>
            <fp-field :name="'writeAcceptorGroup'"
                      :value="node.actor.state.writeAcceptorGroup">
            </fp-field>
            <fp-field :name="'acceptorToRounds'">
              <frankenpaxos-map :map="node.actor.state.acceptorToRounds">
              </frankenpaxos-map>
            </fp-field>
            <fp-field :name="'pendingRounds'"
                      :value="node.actor.state.pendingRounds">
            </fp-field>
            <fp-field :name="'phase1bs'">
              <frankenpaxos-map :map="node.actor.state.phase1bs">
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Phase2')">
          Phase2
          <fp-object>
            <fp-field :name="'v'" :value="node.actor.state.v"></fp-field>
            <fp-field :name="'phase2bs'">
              <frankenpaxos-map :map="node.actor.state.phase2bs">
              </frankenpaxos-map>
            </fp-field>
          </fp-object>
        </div>

        <div v-if="node.actor.state.constructor.name.includes('Chosen')">
          Phase2
          <fp-object>
            <fp-field :name="'v'" :value="node.actor.state.v"></fp-field>
          </fp-object>
        </div>
      </div>
    </div>
  `,
};

const matchmaker_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>
        acceptorGroups =
        <frankenpaxos-map :map="node.actor.acceptorGroups">
        </frankenpaxos-map>
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
        voteRound = {{node.actor.voteRound}}
      </div>

      <div>
        voteValue = {{node.actor.voteValue}}
      </div>
    </div>
  `,
};

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(MatchmakerPaxos, snap) {
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
  const matchmaker_x = 300;
  const acceptor_x = 400;

  const nodes = {};

  // Clients.
  const clients = [
    {client: MatchmakerPaxos.client1, y: 250},
    {client: MatchmakerPaxos.client2, y: 350},
    {client: MatchmakerPaxos.client3, y: 450},
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
    {leader: MatchmakerPaxos.leader1, y: 300},
    {leader: MatchmakerPaxos.leader2, y: 400},
  ]
  for (const [index, {leader, y}] of leaders.entries()) {
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
  }

  // Leaders.
  const matchmakers = [
    {matchmaker: MatchmakerPaxos.matchmaker1, y: 250},
    {matchmaker: MatchmakerPaxos.matchmaker2, y: 350},
    {matchmaker: MatchmakerPaxos.matchmaker3, y: 450},
  ]
  for (const [index, {matchmaker, y}] of matchmakers.entries()) {
    const color = flat_green;
    nodes[matchmaker.address] = {
      actor: matchmaker,
      color: color,
      component: matchmaker_info,
      svgs: [
        snap.circle(matchmaker_x, y, 20).attr(colored(color)),
        snap.text(matchmaker_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: MatchmakerPaxos.acceptor1, y: 100},
    {acceptor: MatchmakerPaxos.acceptor2, y: 200},
    {acceptor: MatchmakerPaxos.acceptor3, y: 300},
    {acceptor: MatchmakerPaxos.acceptor4, y: 400},
    {acceptor: MatchmakerPaxos.acceptor5, y: 500},
    {acceptor: MatchmakerPaxos.acceptor6, y: 600},
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

  // Node titles.
  const anchor_middle = (text) => text.attr({'text-anchor': 'middle'});
  anchor_middle(snap.text(client_x, 50, 'Clients'));
  anchor_middle(snap.text(leader_x, 50, 'Leaders'));
  anchor_middle(snap.text(matchmaker_x, 50, 'Matchmakers'));
  anchor_middle(snap.text(acceptor_x, 50, 'Acceptors'));

  return nodes;
}

function main() {
  const MatchmakerPaxos =
    frankenpaxos.matchmakerpaxos.MatchmakerPaxos.MatchmakerPaxos;
  const snap = Snap('#animation');
  const nodes = make_nodes(MatchmakerPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[MatchmakerPaxos.client1.address],
      transport: MatchmakerPaxos.transport,
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
