load("@bazel_tools//tools/build_defs/cc:action_names.bzl", "ACTION_NAMES")
load("@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl", "artifact_name_pattern")
   # NEW
load(
    "@bazel_tools//tools/cpp:cc_toolchain_config_lib.bzl",
    "feature",
    "flag_group",
    "flag_set",
    "tool_path",
)

all_link_actions = [ # NEW
    ACTION_NAMES.cpp_link_executable,
    ACTION_NAMES.cpp_link_dynamic_library,
    ACTION_NAMES.cpp_link_nodeps_dynamic_library,
]


def _impl(ctx):
    tool_paths = [ # NEW
        tool_path(
            name = "gcc",
            path = "/usr/bin/x86_64-w64-mingw32-gcc",
        ),
        tool_path(
            name = "ld",
            path = "/usr/bin/x86_64-w64-mingw32-ld",
        ),
        tool_path(
            name = "ar",
            path = "/usr/bin/x86_64-w64-mingw32-ld",
        ),
        tool_path(
            name = "cpp",
            path = "/bin/false",
        ),
        tool_path(
            name = "gcov",
            path = "/bin/false",
        ),
        tool_path(
            name = "nm",
            path = "/bin/false",
        ),
        tool_path(
            name = "objdump",
            path = "/bin/false",
        ),
        tool_path(
            name = "strip",
            path = "/bin/false",
        ),
    ]


    features = [ # NEW
        feature(
            name = "default_linker_flags",
            enabled = True,
            flag_sets = [
                flag_set(
                    actions = all_link_actions,
                    flag_groups = ([
                        flag_group(
                            flags = [
                                "-lstdc++",
                            ],
                        ),
                    ]),
                ),
            ],
        ),
    ]

    artifact_name_patterns = [
        artifact_name_pattern(
            category_name = "executable",
            prefix = "",
            extension = ".exe",
        ),
    ]

    return cc_common.create_cc_toolchain_config_info(
        ctx = ctx,
        features = features, # NEW
        cxx_builtin_include_directories = [ # NEW
            "/usr/x86_64-w64-mingw32/include",
            "/usr/lib/gcc/x86_64-w64-mingw32/10-win32/include/c++",
            "/usr/share/mingw-w64/include",
            "/usr/lib/gcc/x86_64-w64-mingw32/10-win32/include",
            "/usr/lib/gcc/x86_64-w64-mingw32/10-win32/include-fixed",
            "/usr/include",
        ],
        toolchain_identifier = "mingw-toolchain",
        host_system_name = "local",
        target_system_name = "windows",
        target_cpu = "x86_64",
        target_libc = "unknown",
        compiler = "x86_64-w64-mingw32-gcc",
        abi_version = "unknown",
        abi_libc_version = "unknown",
        tool_paths = tool_paths,
        artifact_name_patterns=artifact_name_patterns
    )

cc_toolchain_config = rule(
    implementation = _impl,
    attrs = {},
    provides = [CcToolchainConfigInfo],
)