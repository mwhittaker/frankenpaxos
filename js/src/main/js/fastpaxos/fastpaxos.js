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
      <div><strong>proposedValue</strong>: {{node.actor.proposedValue}}</div>
      <div><strong>chosenValue</strong>: {{node.actor.chosenValue}}</div>
      <div><strong>phase2bResponses</strong>: {{node.actor.phase2bResponses}}</div>
      <button v-on:click="propose">Propose</button>
      <input v-model="proposal" v-on:keyup.enter="propose"></input>
    </div>
  `,
};

let leader_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>status</strong>: {{node.actor.status}}</div>
      <div><strong>proposedValue</strong>: {{node.actor.proposedValue}}</div>
      <div><strong>phase1bResponses</strong>:
           <frankenpaxos-set :set="node.actor.phase1bResponses">
           </frankenpaxos-set>
      </div>
      <div><strong>phase2bResponses</strong>:
           <frankenpaxos-set :set="node.actor.phase2bResponses">
           </frankenpaxos-set>
      </div>
      <div><strong>chosenValue</strong>: {{node.actor.chosenValue}}</div>
    </div>
  `,
};

let acceptor_info = {
  props: ['node'],

  template: `
    <div>
      <div><strong>round</strong>: {{node.actor.round}}</div>
      <div><strong>voteRound</strong>: {{node.actor.voteRound}}</div>
      <div><strong>voteValue</strong>: {{node.actor.voteValue}}</div>
    </div>
  `,
};

let abbreviated_acceptor_info = {
  props: ['node'],

  template: `
    <div class="column">
      <div>{{node.actor.address.address}}</div>
      <div>{{node.actor.round}}</div>
      <div>{{node.actor.voteRound}}</div>
      <div>{{node.actor.voteValue}}</div>
    </div>
  `,
};


function make_nodes(FastPaxos, snap) {
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
  nodes[FastPaxos.client1.address] = {
    actor: FastPaxos.client1,
    svgs: [
      snap.circle(50, 100, 20).attr(colored(flat_red)),
      snap.text(50, 102, '1').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[FastPaxos.client2.address] = {
    actor: FastPaxos.client2,
    svgs: [
      snap.circle(50, 200, 20).attr(colored(flat_red)),
      snap.text(50, 202, '2').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }
  nodes[FastPaxos.client3.address] = {
    actor: FastPaxos.client3,
    svgs: [
      snap.circle(50, 300, 20).attr(colored(flat_red)),
      snap.text(50, 302, '3').attr(number_style),
    ],
    color: flat_red,
    component: client_info,
  }

  // Leaders.
  nodes[FastPaxos.leader1.address] = {
    actor: FastPaxos.leader1,
    svgs: [
      snap.circle(200, 50, 20).attr(colored(flat_blue)),
      snap.text(200, 52, '1').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }
  nodes[FastPaxos.leader2.address] = {
    actor: FastPaxos.leader2,
    svgs: [
      snap.circle(200, 350, 20).attr(colored(flat_blue)),
      snap.text(200, 352, '2').attr(number_style),
    ],
    color: flat_blue,
    component: leader_info,
  }

  // Acceptors.
  nodes[FastPaxos.acceptor1.address] = {
    actor: FastPaxos.acceptor1,
    svgs: [
      snap.circle(350, 100, 20).attr(colored(flat_green)),
      snap.text(350, 102, '1').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[FastPaxos.acceptor2.address] = {
    actor: FastPaxos.acceptor2,
    svgs: [
      snap.circle(350, 200, 20).attr(colored(flat_green)),
      snap.text(350, 202, '2').attr(number_style),
    ],
    color: flat_green,
    component: acceptor_info,
  }
  nodes[FastPaxos.acceptor3.address] = {
    actor: FastPaxos.acceptor3,
    svgs: [
      snap.circle(350, 300, 20).attr(colored(flat_green)),
      snap.text(350, 302, '3').attr(number_style),
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
  let FastPaxos = frankenpaxos.fastpaxos.TweenedFastPaxos.FastPaxos;
  let snap = Snap('#tweened_animation');
  let nodes = make_nodes(FastPaxos, snap);

  let vue_app = new Vue({
    el: '#tweened_app',

    components: {
      'abbreviated-acceptor-info': abbreviated_acceptor_info,
    },

    data: {
      nodes: nodes,
      node: nodes[FastPaxos.client1.address],
      acceptor1: nodes[FastPaxos.acceptor1.address],
      acceptor2: nodes[FastPaxos.acceptor2.address],
      acceptor3: nodes[FastPaxos.acceptor3.address],
      transport: FastPaxos.transport,
      time_scale: 1,
      auto_deliver_messages: true,
      auto_start_timers: true,
    },

    methods: {
      send_message: (message, callback) => {
        let src = nodes[message.src];
        let dst = nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = (1000 + Math.random() * 200) / 1000;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        this.nodes[address].svgs[0].attr({fill: "#7f8c8d"});
      },

      unpartition: function(address) {
        this.nodes[address].svgs[0].attr({fill: this.nodes[address].color});
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
