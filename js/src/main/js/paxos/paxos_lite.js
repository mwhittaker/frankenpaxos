let client_info = {
  props: ['node'],

  data: function() {
    return {
      proposal: "",
    };
  },

  methods: {
    optionToJs: function(o) {
      let x = this.JsUtils.optionToJs(o);
      return x === undefined ? "null" : '"' + x + '"';
    },

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
      <div>
        <strong>proposed value</strong>:
        {{optionToJs(node.actor.proposedValue)}}
      </div>
      <div>
        <strong>chosen value</strong>:
        {{optionToJs(node.actor.chosenValue)}}
      </div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let leader_info = {
  props: ['node'],

  methods: {
    optionToJs: function(o) {
      let x = this.JsUtils.optionToJs(o);
      return x === undefined ? "null" : '"' + x + '"';
    },
  },

  template: `
    <div>
      <div>
        <strong>round</strong>:
        {{node.actor.round}}
      </div>
      <div>
        <strong>status</strong>:
        {{node.actor.status}}
      </div>
      <div>
        <strong>proposedValue</strong>:
        {{optionToJs(node.actor.proposedValue)}}
      </div>
      <div>
        <strong>chosenValue</strong>:
        {{optionToJs(node.actor.chosenValue)}}
      </div>
      <!-- <div><strong>phase1bResponses</strong>: -->
      <!--      <frankenpaxos-set :set="node.actor.phase1bResponses"> -->
      <!--      </frankenpaxos-set> -->
      <!-- </div> -->
      <!-- <div><strong>phase2bResponses</strong>: -->
      <!--      <frankenpaxos-set :set="node.actor.phase2bResponses"> -->
      <!--      </frankenpaxos-set> -->
      <!-- </div> -->
    </div>
  `,
};

let acceptor_info = {
  props: ['node'],

  methods: {
    optionToJs: function(o) {
      let x = this.JsUtils.optionToJs(o);
      return x === undefined ? "null" : '"' + x + '"';
    },
  },

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>vote round</strong>: {{node.actor.voteRound}}</div>
      <div>
        <strong>vote value</strong>:
        {{optionToJs(node.actor.voteValue)}}
      </div>
    </div>
  `,
};

let abbreviated_acceptor_info = {
  props: ['node'],

  methods: {
    voteValue: function() {
      let v = this.JsUtils.optionToJs(this.node.actor.voteValue);
      return v === undefined ? "Nothing" : '"' + v + '"';
    },
  },

  template: `
    <div class="column">
      <div><strong>{{node.actor.address.address}}</strong></div>
      <div>{{node.actor.round}}</div>
      <div>{{node.actor.voteRound}}</div>
      <div>{{voteValue()}}</div>
    </div>
  `,
};


function make_nodes(Paxos, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_red = '#e74c3c';
  let flat_blue = '#3498db';
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

  let nodes = {};

  // Clients.
  nodes[Paxos.client1.address] = {
    actor: Paxos.client1,
    svgs: [
      snap.circle(50, 50, 20).attr(colored(flat_red)),
      snap.text(50, 52, '1').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[Paxos.client2.address] = {
    actor: Paxos.client2,
    svgs: [
      snap.circle(50, 150, 20).attr(colored(flat_red)),
      snap.text(50, 152, '2').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[Paxos.client3.address] = {
    actor: Paxos.client3,
    svgs: [
      snap.circle(50, 250, 20).attr(colored(flat_red)),
      snap.text(50, 252, '3').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }

  // Leaders.
  nodes[Paxos.leader1.address] = {
    actor: Paxos.leader1,
    svgs: [
      snap.circle(200, 100, 20).attr(colored(flat_blue)),
      snap.text(200, 102, '1').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }
  nodes[Paxos.leader2.address] = {
    actor: Paxos.leader2,
    svgs: [
      snap.circle(200, 200, 20).attr(colored(flat_blue)),
      snap.text(200, 202, '2').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }

  // Acceptors.
  nodes[Paxos.acceptor1.address] = {
    actor: Paxos.acceptor1,
    svgs: [
      snap.circle(350, 50, 20).attr(colored(flat_green)),
      snap.text(350, 52, '1').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[Paxos.acceptor2.address] = {
    actor: Paxos.acceptor2,
    svgs: [
      snap.circle(350, 150, 20).attr(colored(flat_green)),
      snap.text(350, 152, '2').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[Paxos.acceptor3.address] = {
    actor: Paxos.acceptor3,
    svgs: [
      snap.circle(350, 250, 20).attr(colored(flat_green)),
      snap.text(350, 252, '3').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }

  // Node titles.
  snap.text(50, 15, 'Clients').attr({'text-anchor': 'middle'});
  snap.text(200, 15, 'Leaders').attr({'text-anchor': 'middle'});
  snap.text(350, 15, 'Acceptors').attr({'text-anchor': 'middle'});

  return nodes
}

function main() {
  let Paxos = frankenpaxos.paxos.TweenedPaxos.Paxos;
  let snap = Snap('#tweened_animation');
  let nodes = make_nodes(Paxos, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: '#tweened_app',

    components: {
      'abbreviated-acceptor-info': abbreviated_acceptor_info,
    },

    data: {
      JsUtils: frankenpaxos.JsUtils,
      nodes: nodes,
      node: nodes[Paxos.client1.address],
      acceptor1: nodes[Paxos.acceptor1.address],
      acceptor2: nodes[Paxos.acceptor2.address],
      acceptor3: nodes[Paxos.acceptor3.address],
      transport: Paxos.transport,
      time_scale: 0.3,
      auto_deliver_messages: false,
      auto_start_timers: false,
    },

    methods: {
      send_message: function(message) {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = (250 + Math.random() * 200) / 1000;
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
