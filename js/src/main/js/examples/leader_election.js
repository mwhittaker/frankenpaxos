function make_nodes(LeaderElection, snap) {
  // https://flatuicolors.com/palette/defo
  let flat_light_green = '#2ecc71';
  let flat_green = '#27ae60';
  let flat_blue = '#3498db';
  let flat_red = '#e74c3c';
  let colored = (color) => {
    return {
      'fill': color,
      'stroke': 'black', 'stroke-width': '3pt',
    }
  };

  let polar_to_cartesian = function(x_origin, y_origin, theta, r) {
    let dx = Math.cos(theta) * r;
    let dy = Math.sin(theta) * r;
    return [x_origin + dx, y_origin - dy];
  }

  // Nodes.
  //
  // center 200, 150
  let nodes = {};
  let x_origin = 200;
  let y_origin = 150;
  let theta = 2 * Math.PI / 5;
  let r = 100;

  let [ax, ay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r);
  let [bx, by] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r);
  let [cx, cy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r);
  let [dx, dy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r);
  let [ex, ey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r);

  nodes[LeaderElection.a.address] = {
    actor: LeaderElection.a,
    svgs: [snap.circle(ax, ay, 20).attr(colored(flat_light_green))],
  };
  nodes[LeaderElection.b.address] = {
    actor: LeaderElection.b,
    svgs: [snap.circle(bx, by, 20).attr(colored(flat_light_green))],
  };
  nodes[LeaderElection.c.address] = {
    actor: LeaderElection.c,
    svgs: [snap.circle(cx, cy, 20).attr(colored(flat_light_green))],
  };
  nodes[LeaderElection.d.address] = {
    actor: LeaderElection.d,
    svgs: [snap.circle(dx, dy, 20).attr(colored(flat_light_green))],
  };
  nodes[LeaderElection.e.address] = {
    actor: LeaderElection.e,
    svgs: [snap.circle(ex, ey, 20).attr(colored(flat_light_green))],
  };

  // Node titles.
  [ax, ay] = polar_to_cartesian(x_origin, y_origin, 1 * theta, r + 40);
  [bx, by] = polar_to_cartesian(x_origin, y_origin, 2 * theta, r + 40);
  [cx, cy] = polar_to_cartesian(x_origin, y_origin, 3 * theta, r + 40);
  [dx, dy] = polar_to_cartesian(x_origin, y_origin, 4 * theta, r + 40);
  [ex, ey] = polar_to_cartesian(x_origin, y_origin, 5 * theta, r + 40);
  let title_attr = {'text-anchor': 'middle', 'alignment-baseline': 'middle'}
  snap.text(ax, ay, 'a').attr(title_attr);
  snap.text(bx, by, 'b').attr(title_attr);
  snap.text(cx, cy, 'c').attr(title_attr);
  snap.text(dx, dy, 'd').attr(title_attr);
  snap.text(ex, ey, 'e').attr(title_attr);

  return nodes
}

function make_app(LeaderElection, snap, app_id) {
  let nodes = make_nodes(LeaderElection, snap);

  // Create the vue app.
  let vue_app = new Vue({
    el: app_id,

    data: {
      JsUtils: zeno.JsUtils,
      node: nodes[LeaderElection.a.address],
      transport: LeaderElection.transport,
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
  make_app(zeno.examples.js.SimulatedLeaderElection.LeaderElection,
           Snap('#simulated_animation'),
           '#simulated_app');

  make_app(zeno.examples.js.ClickthroughLeaderElection.LeaderElection,
           Snap('#clickthrough_animation'),
           '#clickthrough_app');
}

window.onload = main
