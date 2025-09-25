/// Debug-only pointer alignment assertion that is safe to export.
///
/// Why this style:
/// - We need to re-export a symbol other crates can call, but we do not
///   want benches or release builds to pull in debug-only deps or code.
/// - Putting `#[cfg(...)]` on the function itself makes the symbol
///   vanish in release/bench. Callers would then need their own cfg
///   fences, which is brittle across crates.
/// - By keeping the function always present and gating only its body,
///   callers can invoke it unconditionally. In debug/test it asserts;
///   in release/bench it compiles to a no-op.
///
/// Build behavior:
/// - In debug/test, the inner block runs and uses `debug_assert!`.
/// - In release/bench, the else block keeps the args "used" so the
///   function is a true no-op (no codegen warnings, no panic paths).
///
/// Cost:
/// - Inlining plus the cfg-ed body means zero runtime cost in release
///   and bench profiles.
///
/// Usage:
/// - Call anywhere you want a cheap alignment check in debug/test,
///   including from other crates that depend on this one.
#[inline]
pub fn debug_assert_aligned(ptr: *const u8, align: usize) {
    #[cfg(any(test, debug_assertions))]
    {
        debug_assert!(align.is_power_of_two());
        debug_assert!(
            (ptr as usize & (align - 1)) == 0,
            "buffer base is not {}-byte aligned",
            align
        );
    }

    #[cfg(not(any(test, debug_assertions)))]
    {
        // Release/bench: no-op. Keep args used to avoid warnings.
        let _ = ptr;
        let _ = align;
    }
}

/// Debug-only file-offset alignment assertion that is safe to export.
///
/// Same rationale as `debug_assert_aligned`: keep a stable symbol that
/// callers can invoke without cfg fences, while ensuring zero cost in
/// release/bench builds.
///
/// Why not a module-level cfg or `use`:
/// - Some bench setups compile with `--all-features` and may still pull
///   modules in ways that trip cfg-ed imports. Gating inside the body
///   avoids those hazards and keeps the bench linker happy.
///
/// Behavior:
/// - Debug/test: checks that `off` is a multiple of the configured
///   `PAYLOAD_ALIGNMENT`.
/// - Release/bench: no-op, arguments are marked used.
///
/// Notes:
/// - This asserts the *derived start offset* of a payload, not the
///   pointer. Use the pointer variant to assert the actual address you
///   hand to consumers like Arrow.
#[inline]
pub fn debug_assert_aligned_offset(off: u64) {
    #[cfg(any(test, debug_assertions))]
    {
        use crate::constants::PAYLOAD_ALIGNMENT;

        debug_assert!(
            PAYLOAD_ALIGNMENT.is_power_of_two(),
            "PAYLOAD_ALIGNMENT must be a power of two"
        );
        debug_assert!(
            off.is_multiple_of(PAYLOAD_ALIGNMENT),
            "derived payload start not {}-byte aligned (got {})",
            PAYLOAD_ALIGNMENT,
            off
        );
    }

    #[cfg(not(any(test, debug_assertions)))]
    {
        // Release/bench: no-op. Keep arg used to avoid warnings.
        let _ = off;
    }
}
