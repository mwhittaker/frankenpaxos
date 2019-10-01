// Returns numbers drawn from an exponential distribution with mean `mean`.
let exponential = function(mean) {
  return -Math.log(Math.random() + Number.EPSILON) * mean
}

let election_colors = {
  leaderless_follower: '#f1c40f',
  follower: '#27ae60',
  candidate: '#3498db',
  leader: '#e74c3c',
}

function make_nodes(Election, snap) {
  // https://flatuicolors.com/palette/defo
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black',
      'stroke-width': '3pt',
    }
  };

  let polar_to_cartesian = function(x_origin, y_origin, theta, r) {
    let dx = Math.cos(theta) * r;
    let dy = Math.sin(theta) * r;
    return [x_origin + dx, y_origin - dy];
  }

  let nodes = {};
  let title_attr = {'text-anchor': 'middle', 'alignment-baseline': 'middle'};
  let x_origin = 200;
  let y_origin = 150;
  let theta = 2 * Math.PI / 5;
  let r = 100;

  // Node positions.
  let [ax, ay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r);
  let [bx, by] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r);
  let [cx, cy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r);
  let [dx, dy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r);
  let [ex, ey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r);

  // Title positions.
  let [tax, tay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r + 40);
  let [tbx, tby] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r + 40);
  let [tcx, tcy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r + 40);
  let [tdx, tdy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r + 40);
  let [tex, tey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r + 40);

  nodes[Election.a.address] = {
    actor: Election.a,
    svgs: [
      snap.circle(ax, ay, 20).attr(colored(election_colors.leaderless_follower)),
      snap.text(ax, ay, '0').attr(title_attr),
      snap.text(tax, tay, 'a').attr(title_attr),
    ],
  };
  nodes[Election.b.address] = {
    actor: Election.b,
    svgs: [
      snap.circle(bx, by, 20).attr(colored(election_colors.leaderless_follower)),
      snap.text(bx, by, '0').attr(title_attr),
      snap.text(tbx, tby, 'b').attr(title_attr),
    ],
  };
  nodes[Election.c.address] = {
    actor: Election.c,
    svgs: [
      snap.circle(cx, cy, 20).attr(colored(election_colors.leaderless_follower)),
      snap.text(cx, cy, '0').attr(title_attr),
      snap.text(tcx, tcy, 'c').attr(title_attr),
    ],
  };
  nodes[Election.d.address] = {
    actor: Election.d,
    svgs: [
      snap.circle(dx, dy, 20).attr(colored(election_colors.leaderless_follower)),
      snap.text(dx, dy, '0').attr(title_attr),
      snap.text(tdx, tdy, 'd').attr(title_attr),
    ],
  };
  nodes[Election.e.address] = {
    actor: Election.e,
    svgs: [
      snap.circle(ex, ey, 20).attr(colored(election_colors.leaderless_follower)),
      snap.text(ex, ey, '0').attr(title_attr),
      snap.text(tex, tey, 'e').attr(title_attr),
    ],
  };

  return nodes
}

function main() {
  let Election =
    frankenpaxos.election.raft.TweenedElection.Election;
  let snap = Snap('#animation');
  let nodes = make_nodes(Election, snap);

  let state_to_color = function(state) {
    // scala.js does not let you nicely pattern match on an ADT. Thus, we do
    // something hacky and inspect the name of the constructor.
    let name = state.constructor.name;
    if (name.includes('Participant$LeaderlessFollower')) {
      return election_colors.leaderless_follower;
    } else if (name.includes('Participant$Follower')) {
      return election_colors.follower;
    } else if (name.includes('Participant$Candidate')) {
      return election_colors.candidate;
    } else if (name.includes('Participant$Leader')) {
      return election_colors.leader;
    }
  };

  let node_watch = {
    deep: true,
    handler: function(node) {
      node.svgs[0].attr({fill: state_to_color(node.actor.state)});
      node.svgs[1].attr({text: node.actor.round});
    },
  }

  // Create the vue app.
  let vue_app = new Vue({
    el: '#app',

    data: {
      nodes: nodes,
      node: nodes[Election.a.address],
      transport: Election.transport,
      settings: {
          time_scale: 1,
          auto_deliver_messages: true,
          auto_start_timers: true,
      },
      a: nodes[Election.a.address],
      b: nodes[Election.b.address],
      c: nodes[Election.c.address],
      d: nodes[Election.d.address],
      e: nodes[Election.e.address],
    },

    methods: {
      send_message: function(message) {
        let src = this.nodes[message.src];
        let dst = this.nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");

        let svg_message = snap.circle(src_x, src_y, 9).attr({fill: '#2c3e50'});
        snap.prepend(svg_message);
        let duration = (500 + exponential(500)) / 1000;
        return TweenMax.to(svg_message.node, duration, {
          attr: { cx: dst_x, cy: dst_y },
          ease: Linear.easeNone,
          onComplete: () => { svg_message.remove(); },
        });
      },

      partition: function(address) {
        this.nodes[address].svgs[2].attr({fill: "#7f8c8d"});
      },

      unpartition: function(address) {
        this.nodes[address].svgs[2].attr({fill: "black"});
      },
    },

    watch: {
      a: node_watch,
      b: node_watch,
      c: node_watch,
      d: node_watch,
      e: node_watch,
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
