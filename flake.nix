{
  description = "A flake for Zig development with Zig Language Server";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    zig-overlay.url = "github:mitchellh/zig-overlay";
  };

  outputs = { self, nixpkgs, zig-overlay }:
    let
      pkgs = import nixpkgs {
        overlays = [ zig-overlay.overlay ];
        inherit (self) system;
      };
    in
    {
      devShells.default = pkgs.mkShell {
        buildInputs = [
          pkgs.zig
          pkgs.zls
        ];
      };
    };
}