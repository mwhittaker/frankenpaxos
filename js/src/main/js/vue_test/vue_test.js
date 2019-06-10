function main() {
  let VueTest = frankenpaxos.vue_test.TweenedVueTest.VueTest;

  // Create the vue app.
  let vue_app = new Vue({
    el: '#tweened_app',

    data: {
      VueTest: VueTest,
      key: "",
      value: "",
    },

    methods: {
      directAddMutableString: function() {
        this.VueTest.directAddMutableString(this.key, this.value);
        this.key = "";
        this.value = "";
      },
    },
  });
}

window.onload = main
