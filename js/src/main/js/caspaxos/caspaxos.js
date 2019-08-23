// Helper components ///////////////////////////////////////////////////////////
const phase1b_component = {
  props: {
    value: Object,
  },

  computed: {
    voteValue: function() {
      return JsUtils.optionToJs(value.voteRound);
    }
  },

  template: `
    <fp-object>
      <fp-field :name="'round'" :value="'value.round'"></fp-field>
      <fp-field :name="'acceptorIndex'" :value="'value.acceptorIndex'"></fp-field>
      <fp-field :name="'voteRound'" :value="'value.voteRound'"></fp-field>
      <fp-field :name="'voteValue'" :value="'voteValue()'"></fp-field>
    </fp-object>
  `,
};

const phase2b_component = {
  props: {
    value: Object,
  },

  template: `
    <fp-object>
      <fp-field :name="'vertexId'" :value="'value.vertexId'"></fp-field>
      <fp-field :name="'acceptorId'" :value="'value.acceptorId'"></fp-field>
      <fp-field :name="'round'" :value="'value.round'"></fp-field>
    </fp-object>
  `,
};

const client_request_component = {
  props: {
    value: Object,
  },

  template: `
    <div>
      <fp-object>
        <fp-field :name="'clientAddress'"
                  :value="value.clientAddress"></fp-field>
        <fp-field :name="'clientId'"
                  :value="value.clientId"></fp-field>
        <fp-field :name="'intSet'"
                  :value="value.intSet.value"></fp-field>
      </fp-object>
    </div>
  `,
};

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
      <div v-if="node.actor.state.constructor.name.endsWith('Idle')">
        Idle
        <fp-object>
          <fp-field :name="'id'" :value="node.actor.state.id"></fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.endsWith('Pending')">
        Pending
        <fp-object>
          <fp-field :name="'id'"
                    :value="node.actor.state.id"></fp-field>
          <fp-field :name="'promise'"
                    :value="node.actor.state.promise"></fp-field>
          <fp-field :name="'resendClientRequest'"
                    :value="node.actor.state.resendClientRequest"></fp-field>
        </fp-object>
      </div>

      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
}

let leader_info = {
  props: {
    node: Object,
  },

  components: {
    'client-request': client_request_component,
    'phase1b': phase1b_component,
    'phase2b': phase2b_component,
  },

  template: `
    <div>
      <div v-if="node.actor.state.constructor.name.endsWith('Idle')">
        Idle
        <fp-object>
          <fp-field :name="'round'" :value="node.actor.state.round"></fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.endsWith('Phase1')">
        Phase1
        <fp-object>
          <fp-field :name="'clientRequests'">
            <frankenpaxos-seq :seq="node.actor.state.clientRequests"
                              v-slot="{value: value}">
              <client-request :value="value"></client-request>
            </frankenpaxos-seq>
          </fp-field>
          <fp-field :name="'round'"
                    :value="node.actor.state.round"></fp-field>
          <fp-field :name="'phase1bs'">
            <frankenpaxos-map :map="node.actor.state.phase1bs"
                              v-slot="{value: value}">
              <phase1b :value="value"></phase1b>
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendPhase1as'"
                    :value="node.actor.state.resendPhase1as"></fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.endsWith('Phase2')">
        Phase2
        <fp-object>
          <fp-field :name="'clientRequests'">
            <frankenpaxos-seq :seq="node.actor.state.clientRequests"
                              v-slot="{value: value}">
              <client-request :value="value"></client-request>
            </frankenpaxos-seq>
          </fp-field>
          <fp-field :name="'round'"
                    :value="node.actor.state.round"></fp-field>
          <fp-field :name="'value'"
                    :value="node.actor.state.value"></fp-field>
          <fp-field :name="'phase2bs'">
            <frankenpaxos-map :map="node.actor.state.phase2bs"
                              v-slot="{value: value}">
              <phase2b :value="value"></phase2b>
            </frankenpaxos-map>
          </fp-field>
          <fp-field :name="'resendPhase2as'"
                    :value="node.actor.state.resendPhase2as"></fp-field>
        </fp-object>
      </div>

      <div v-if="node.actor.state.constructor.name.endsWith('WaitingToRecover')">
        WaitingToRecover
        <fp-object>
          <fp-field :name="'clientRequests'">
            <frankenpaxos-seq :seq="node.actor.state.clientRequests"
                              v-slot="{value: value}">
              <client-request :value="value"></client-request>
            </frankenpaxos-seq>
          </fp-field>
          <fp-field :name="'round'"
                    :value="node.actor.state.round"></fp-field>
          <fp-field :name="'recoverTimer'"
                    :value="node.actor.state.recoverTimer"></fp-field>
        </fp-object>
      </div>
    </div>
  `,
};;

let acceptor_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>round = {{node.actor.round}}</div>
      <div>voteRound = {{node.actor.voteRound}}</div>
      <div>voteValue = {{node.actor.voteValue}}</div>
    </div>
  `,
};

// Main app ////////////////////////////////////////////////////////////////////
function make_nodes(CasPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  const flat_red = '#e74c3c';
  const flat_blue = '#3498db';
  const flat_green = '#2ecc71';

  const colored = (color) => ({
    'fill': color,
    'stroke': 'black',
    'stroke-width': '3pt',
  });

  const number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  const client_x = 50;
  const leader_x = 200;
  const acceptor_x = 350;

  let nodes = {};

  // Clients.
  const clients = [
    {client: CasPaxos.client1, y: 100},
    {client: CasPaxos.client2, y: 200},
    {client: CasPaxos.client3, y: 300},
  ]
  for (const [index, {client, y}] of clients.entries()) {
    nodes[client.address] = {
      actor: client,
      color: flat_red,
      component: client_info,
      svgs: [
        snap.circle(client_x, y, 20).attr(colored(flat_red)),
        snap.text(client_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Leaders.
  const leaders = [
    {leader: CasPaxos.leader1, y: 100},
    {leader: CasPaxos.leader2, y: 200},
    {leader: CasPaxos.leader3, y: 300},
  ]
  for (const [index, {leader, y}] of leaders.entries()) {
    nodes[leader.address] = {
      actor: leader,
      color: flat_blue,
      component: leader_info,
      svgs: [
        snap.circle(leader_x, y, 20).attr(colored(flat_blue)),
        snap.text(leader_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Acceptors.
  const acceptors = [
    {acceptor: CasPaxos.acceptor1, y: 100},
    {acceptor: CasPaxos.acceptor2, y: 200},
    {acceptor: CasPaxos.acceptor3, y: 300},
  ]
  for (const [index, {acceptor, y}] of acceptors.entries()) {
    nodes[acceptor.address] = {
      actor: acceptor,
      color: flat_green,
      component: acceptor_info,
      svgs: [
        snap.circle(acceptor_x, y, 20).attr(colored(flat_green)),
        snap.text(acceptor_x, y, (index + 1).toString()).attr(number_style),
      ],
    };
  }

  // Node titles.
  snap.text(client_x, 50, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(leader_x, 50, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(acceptor_x, 50, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes;
}

function main() {
  const CasPaxos = frankenpaxos.caspaxos.CasPaxos.CasPaxos;
  const snap = Snap('#animation');
  const nodes = make_nodes(CasPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[CasPaxos.client1.address],
      transport: CasPaxos.transport,
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
  for (let node of Object.values(nodes)) {
    for (let svg of node.svgs) {
      svg.node.onclick = () => {
        vue_app.node = node;
      }
    }
  }
}

window.onload = main
