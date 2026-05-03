{
  description = "Filesystem-backed Kubernetes API server experiment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Pin Go explicitly. Bump if k8s.io/* libs we end up depending on
        # require a newer toolchain.
        go = pkgs.go_1_25;
      in
      {
        devShells.default = pkgs.mkShell {
          name = "k8s-api-server-experiment";

          packages = with pkgs; [
            # --- Go toolchain ---
            go            # compiler, gofmt, go test
            gopls         # LSP
            gotools       # goimports, godoc
            delve         # dlv debugger
            golangci-lint # linter

            # --- Kubernetes client ---
            kubectl

            # --- GitHub CLI ---
            gh

            # --- Container runtime ---
            # NOTE: only the Docker CLI is provided here. The Docker *daemon*
            # must be running on the host (systemd, Docker Desktop, colima,
            # etc.). kubelet-lite talks to the daemon socket directly.
            docker-client

            # --- Shell / harness ---
            gnumake
            git
            jq
            tree
            coreutils
          ];

          shellHook = ''
            echo "── k8s-api-server experiment ──────────────────────────"
            echo "  go:      $(${go}/bin/go version | awk '{print $3}')"
            echo "  kubectl: $(kubectl version --client=true -o json 2>/dev/null | jq -r .clientVersion.gitVersion 2>/dev/null || echo 'present')"
            if docker info >/dev/null 2>&1; then
              echo "  docker:  daemon reachable"
            else
              echo "  docker:  ⚠️  daemon NOT reachable — kubelet-lite will fail"
              echo "           (NixOS: virtualisation.docker.enable = true;"
              echo "            other: start Docker Engine / Docker Desktop"
              echo "            and add your user to the 'docker' group)"
            fi
            echo "───────────────────────────────────────────────────────"

            # Keep go install bins inside the project so the dev shell stays
            # self-contained. Anything `go install`d (e.g. openapi-gen) lands
            # in ./.gopath/bin and is on PATH.
            export GOPATH="$PWD/.gopath"
            export PATH="$GOPATH/bin:$PATH"
            mkdir -p "$GOPATH/bin"

            # Default kubeconfig used by the harness.
            export KUBECONFIG="$PWD/run/kubeconfig"
          '';
        };

        # Convenience: `nix fmt` formats the flake itself.
        formatter = pkgs.nixpkgs-fmt;
      });
}
