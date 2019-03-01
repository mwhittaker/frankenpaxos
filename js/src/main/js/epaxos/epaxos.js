let client_info = {
  props: ['node'],

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
    }
  },

  template: `
    <div>
      <div>proposed value: {{node.actor.proposedValue}}</div>
      <div>state: {{node.actor.state}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let replica_info = {
  props: ['node'],

  template: `
    <div>
      <div>nextAvailableInstance: {{node.actor.nextAvailableInstance}}</div> 
      <div>stateMachine: {{node.actor.stateMachine}}</div>
      <div>commands: {{node.actor.commands}}</div>
      <div>preacceptResponses: {{node.actor.preacceptResponses}}</div>
      <div>acceptOkResponses: {{node.actor.acceptOkResponses}}</div>
      <div>interferenceData: {{node.actor.interferenceData}}</div>
    </div>
  `,
};

function make_nodes(EPaxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
  let flat_orange = '#f39c12';
  let flat_green = '#2ecc71';
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  let number_style = {
    'text-anchor': 'middle',
    'alignment-baseline': 'middle',
    'font-size': '20pt',
    'font-weight': 'bolder',
    'fill': 'black',
    'stroke': 'white',
    'stroke-width': '1px',
  }

  let client_x = 50;
  let replica_x = 150;

  let nodes = {};

  // Clients.
  nodes[EPaxos.client1.address] = {
    actor: EPaxos.client1,
    svgs: [
      snap.circle(client_x, 50, 20).attr(colored(flat_red)),
      snap.text(client_x, 52, '1').attr(number_style),
    ],
  };
  nodes[EPaxos.client2.address] = {
    actor: EPaxos.client2,
    svgs: [
      snap.circle(client_x, 150, 20).attr(colored(flat_red)),
      snap.text(client_x, 152, '2').attr(number_style),
    ],
  };
  nodes[EPaxos.client3.address] = {
    actor: EPaxos.client3,
    svgs: [
      snap.circle(client_x, 250, 20).attr(colored(flat_red)),
      snap.text(client_x, 252, '3').attr(number_style),
    ],
  };

  // Replicas.
  nodes[EPaxos.replica1.address] = {
    actor: EPaxos.replica1,
    svgs: [
      snap.circle(replica_x, 100, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 102, '1').attr(number_style),
    ],
  };
  nodes[EPaxos.replica2.address] = {
    actor: EPaxos.replica2,
    svgs: [
      snap.circle(replica_x, 200, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 202, '2').attr(number_style),
    ],
  };
  nodes[EPaxos.replica3.address] = {
    actor: EPaxos.replica3,
    svgs: [
      snap.circle(replica_x, 300, 20).attr(colored(flat_blue)),
      snap.text(replica_x, 302, '3').attr(number_style),
    ],
  };

  // Node titles.
  snap.text(client_x, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(replica_x, 15, 'Replicas').attr({'text-anchor': 'middle'});

  return nodes;
}

function make_app(EPaxos, snap, app_id) {
  let nodes = make_nodes(EPaxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    // components: {
    //   'abbreviated-acceptor-info': abbreviated_acceptor_info,
    // },

    data: {
      // JsUtils: frankenpaxos.JsUtils,
      // acceptor1: nodes[Paxos.acceptor1.address],
      // acceptor2: nodes[Paxos.acceptor2.address],
      // acceptor3: nodes[Paxos.acceptor3.address],
      node: nodes[EPaxos.client1.address],
      transport: EPaxos.transport,
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let svg_message =
          snap.circle(src.svgs[0].attr("cx"), src.svgs[0].attr("cy"), 9)
              .attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        svg_message.animate(
          {cx: dst.svgs[0].attr("cx"), cy: dst.svgs[0].attr("cy")},
          250 + Math.random() * 200,
          callback);
      }
    },

    computed: {
      current_component: function() {
        if (this.node.actor.address.address.includes('Client')) {
          return client_info;
        } else if (this.node.actor.address.address.includes('Replica')) {
          return replica_info;
        } else {
          // Impossible!
          console.assert(false);
        }
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

function main() {
  let epaxos = frankenpaxos.epaxos;
  make_app(epaxos.SimulatedEPaxos.EPaxos,
           Snap('#simulated_animation'),
           '#simulated_app');
  make_app(epaxos.ClickthroughEPaxos.EPaxos,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main