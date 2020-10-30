# Installs pre-requisites for macos.
#
# Most of this comes from:
# https://stackoverflow.com/questions/51161225/how-can-i-make-macos-frameworks-available-to-clang-in-a-nix-environment
#
# TODO: make sure this works on Linux as well
#

let
    pkgs = import <nixpkgs> {};
    frameworks = pkgs.darwin.apple_sdk.frameworks;
in pkgs.stdenv.mkDerivation {
    name = "kubefs";

    buildInputs = [
                    frameworks.Security
                    frameworks.CoreFoundation
                    frameworks.CoreServices
                    pkgs.pkg-config
                    pkgs.osxfuse
                  ];

    # This was part of the original comment on the Stackoverflow response, but not needed for kubefs.
    # shellHook = ''
    #     export NIX_LDFLAGS="-F${frameworks.CoreFoundation}/Library/Frameworks -framework CoreFoundation $NIX_LDFLAGS";
    # '';
}
