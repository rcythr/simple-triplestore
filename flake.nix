{
  description = "creates a dev container for triplestore";

  inputs = {
    flake-utils.url = "github:numtide/flake-utils"; # Utility functions for Nix flakes
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable"; # Main Nix package repository
    rust-overlay.url = "github:oxalica/rust-overlay?rev=260ff391290a2b23958d04db0d3e7015c8417401";
      rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
      rust-overlay.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default];
        };
        # This is needed since VSCode Devcontainers need the following files in order to function.
        baseInfo = with pkgs; [
          # Set up shadow file with user information
          (writeTextDir "etc/shadow" ''
            root:!x:::::::
          '')
          # Set up passwd file with user information
          (writeTextDir "etc/passwd" ''
            root:x:0:0::/root:${runtimeShell}
          '')
          # Set up group file with user information
          (writeTextDir "etc/group" ''
            root:x:0:
          '')
          # Set up gshadow file with user information
          (writeTextDir "etc/gshadow" ''
            root:x::
          '')
          # Set up os-release file with NixOS information, since it is nix the check requirements
          # step for the dev container creation will skip.
          (writeTextDir "etc/os-release" ''
            NAME="NixOS"
            ID=nixos
            VERSION="unstable"
            VERSION_CODENAME=unstable
            PRETTY_NAME="NixOS (unstable)"
            HOME_URL="https://nixos.org/"
            SUPPORT_URL="https://nixos.org/nixos/manual/"
            BUG_REPORT_URL="https://github.com/NixOS/nixpkgs/issues"
          '')
        ];

        myEnv = pkgs.buildEnv {
          name = "my-env";
          paths = with pkgs; [
            # -- Basic Required Files --
            bash # Basic bash to run bare essnetial code
            coreutils-full # Essential GNU utilities (ls, cat, etc.)

            # -- Needed for VSCode dev container --
            gnutar # GNU version of tar for archiving 
            gzip # Compression utility
            gnugrep # GNU version of grep for searching text
            gnused # GNU version of sed for text processing
            pkgs.stdenv.cc.cc.lib # Standard C library needed for linking C++ programs

            # -- OpenSSL --
            cacert
            openssl
            openssl.dev

            # -- Development tools --
            code-server
            which
            curl
            lsof
            strace
            ripgrep
            tree
            tree-sitter
            nix
            git
            fzf
            fd
            eza
            deterministic-uname
            findutils
            gnugrep
            getent
            gawk
            ps

            # -- Compilers, Etc. --
            gcc
            grc
            cmake
            gnumake
            libclang
            python3

            # -- Rust --
            (lib.meta.hiPrio rust-bin.nightly.latest.default)
            rustup
            pkg-config
          ];
          pathsToLink = [
            "/bin"
            "/etc/ssl/certs"
          ];
        };

      in
      {
        packages.default = pkgs.dockerTools.buildImage {
          name = "triplestore-dev";
          tag = "latest";
          copyToRoot = [ myEnv ] ++ baseInfo; 
          config = {
            WorkingDir = "/workspace";
            Env = [
              # Add certificates to allow for cargo to download files
              "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              "SSL_CERT_DIR=/etc/ssl/certs"
              "CARGO_HTTP_CAINFO=/etc/ssl/certs/ca-bundle.crt"
              "CC=gcc" # Set GCC as the default C compiler
              "CXX=g++" # Set G++ as the default C++ compiler
              # Library path for dynamic linking
              "LD_LIBRARY_PATH=${pkgs.stdenv.cc.cc.lib}/lib"
              # Add openssl to pkg config to ensure that it loads for cargo build
              "PKG_CONFIG_PATH=${pkgs.openssl.dev}/lib/pkgconfig"
              # Setting PATH to include essential binaries
              "PATH=/bin:/usr/bin:${myEnv}/bin:/root/.cargo/bin"
              "USER=root" # Setting user to root
              "COREUTILS=${pkgs.coreutils-full}"
              "CMAKE=/bin/cmake"
              "CMAKE_MAKE_PROGRAM=/bin/make"
              "LIBCLANG_PATH=${pkgs.libclang.lib}/lib/"
            ];
            Volumes = { };
            Cmd = [ "/bin/bash" ];
          };
          extraCommands = ''
            # Link the env binary (needed for the check requirements script)
            mkdir -p usr/bin/
            ln -n bin/env usr/bin/env 

            # Link the dynamic linker/loader (needed for Node within vscode server)
            mkdir -p lib64 
            ln -s ${pkgs.glibc}/lib/ld-linux-x86-64.so.2 lib64/ld-linux-x86-64.so.2 

            # Create /tmp dir
            mkdir -p tmp
          '';
        };
      }
    );
}
