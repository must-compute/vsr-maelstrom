{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [ 
    pkgs.git 
    pkgs.gnuplot
    pkgs.graphviz
    pkgs.openjdk17
    pkgs.wget
    pkgs.delta
    pkgs.tree
    pkgs.glibcLocales
    pkgs.lnav
  ];

  # https://devenv.sh/languages/
  languages.rust = {
    enable = true;
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
  };


  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  scripts.install-maelstrom.exec = ''
      wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
      tar -xf maelstrom.tar.bz2
      rm maelstrom.tar.bz2
     '';

  scripts.configure-git.exec = ''
    cat <<EOF >> .git/config
    [core]
        pager = delta --line-numbers --side-by-side
    [interactive]
        diffFilter = delta --color-only --line-numbers --side-by-side
    [delta]
        navigate = true
    [merge]
        conflictStyle = zdiff3
    EOF

    echo "updated local config in .git/config"
'';

  scripts.logs.exec = ''
    lnav  store/latest/node-logs/
     '';

  enterShell = ''
    hello
    git --version
  '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  enterTest = ''
    echo "Running tests"
    git --version | grep --color=auto "${pkgs.git.version}"
  '';

  # https://devenv.sh/pre-commit-hooks/
  # pre-commit.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
