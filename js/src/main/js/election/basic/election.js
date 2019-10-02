const leader_color = '#2ecc71'; // green
const follower_color = '#e74c3c'; // red

const participant_info = {
  props: {
    node: Object,
  },

  template: `
    <div>
      <div>state = {{node.actor.state}}</div>
      <div>round = {{node.actor.round}}</div>
      <div>leaderIndex = {{node.actor.leaderIndex}}</div>
    </div>
  `,
}

function make_nodes(Election, snap) {
  const colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black',
      'stroke-width': '3pt',
    }
  };

  const polar_to_cartesian = function(x_origin, y_origin, theta, r) {
    const dx = Math.cos(theta) * r;
    const dy = Math.sin(theta) * r;
    return [x_origin + dx, y_origin - dy];
  }

  const title_attr = {'text-anchor': 'middle', 'alignment-baseline': 'middle'};
  const x_origin = 200;
  const y_origin = 150;
  const theta = 2 * Math.PI / 5;
  const r = 100;

  // Node positions.
  const [ax, ay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r);
  const [bx, by] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r);
  const [cx, cy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r);
  const [dx, dy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r);
  const [ex, ey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r);

  // Title positions.
  const [tax, tay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r + 40);
  const [tbx, tby] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r + 40);
  const [tcx, tcy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r + 40);
  const [tdx, tdy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r + 40);
  const [tex, tey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r + 40);

  // Nodes.
  const nodes = {};
  nodes[Election.a.address] = {
    actor: Election.a,
    component: participant_info,
    svgs: [
      snap.circle(ax, ay, 20).attr(colored(leader_color)),
      snap.text(ax, ay, '0').attr(title_attr),
      snap.text(tax, tay, 'a').attr(title_attr),
    ],
  };
  nodes[Election.b.address] = {
    actor: Election.b,
    component: participant_info,
    svgs: [
      snap.circle(bx, by, 20).attr(colored(follower_color)),
      snap.text(bx, by, '0').attr(title_attr),
      snap.text(tbx, tby, 'b').attr(title_attr),
    ],
  };
  nodes[Election.c.address] = {
    actor: Election.c,
    component: participant_info,
    svgs: [
      snap.circle(cx, cy, 20).attr(colored(follower_color)),
      snap.text(cx, cy, '0').attr(title_attr),
      snap.text(tcx, tcy, 'c').attr(title_attr),
    ],
  };
  nodes[Election.d.address] = {
    actor: Election.d,
    component: participant_info,
    svgs: [
      snap.circle(dx, dy, 20).attr(colored(follower_color)),
      snap.text(dx, dy, '0').attr(title_attr),
      snap.text(tdx, tdy, 'd').attr(title_attr),
    ],
  };
  nodes[Election.e.address] = {
    actor: Election.e,
    component: participant_info,
    svgs: [
      snap.circle(ex, ey, 20).attr(colored(follower_color)),
      snap.text(ex, ey, '0').attr(title_attr),
      snap.text(tex, tey, 'e').attr(title_attr),
    ],
  };

  return nodes;
}

function main() {
  const Election = frankenpaxos.election.basic.TweenedElection.Election;
  const snap = Snap('#animation');
  const nodes = make_nodes(Election, snap);

  const state_to_color = (state) => {
    if (state.constructor.name.includes('Leader')) {
      return leader_color;
    } else {
      return follower_color;
    }
  };

  const node_watch = {
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
      distance: function(x1, y1, x2, y2) {
        const dx = x1 - x2;
        const dy = y1 - y2;
        return Math.sqrt(dx*dx + dy*dy);
      },

      send_message: function(message) {
        let src = this.nodes[message.src];
        let dst = this.nodes[message.dst];
        let src_x = src.svgs[0].attr("cx");
        let src_y = src.svgs[0].attr("cy");
        let dst_x = dst.svgs[0].attr("cx");
        let dst_y = dst.svgs[0].attr("cy");
        let d = this.distance(src_x, src_y, dst_x, dst_y);
        let speed = 100 + (Math.random() * 50); // px per second.

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
