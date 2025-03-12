#[allow(unused_imports)]
use log::warn;

use std::sync::Once;

// Static variable to ensure the warning is logged only once
#[allow(dead_code)]
static LOG_ONCE: Once = Once::new();

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Performs SIMD-accelerated memory copy using AVX2 on x86_64.
///
/// This function utilizes **AVX2 (Advanced Vector Extensions 2)** to copy memory
/// in **32-byte chunks**, significantly improving performance on supported CPUs.
///
/// # Safety
/// - Requires the `avx2` feature to be **enabled at runtime**.
/// - **Caller must ensure** that `dst` and `src` have at least `len` bytes of valid memory.
///
/// # Parameters
/// - `dst`: Mutable destination slice.
/// - `src`: Source slice.
///
/// # Performance
/// - Processes **32 bytes at a time**.
/// - Falls back to a scalar copy for any remaining bytes.
#[inline]
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn simd_copy_x86(dst: &mut [u8], src: &[u8]) {
    let len = dst.len().min(src.len());
    let chunks = len / 32; // AVX2 processes 32 bytes at a time

    let mut i = 0;
    while i < chunks * 32 {
        let data = _mm256_loadu_si256(src.as_ptr().add(i) as *const __m256i);
        _mm256_storeu_si256(dst.as_mut_ptr().add(i) as *mut __m256i, data);
        i += 32;
    }

    // Copy remaining bytes normally
    dst[i..len].copy_from_slice(&src[i..len]);
}

/// Performs SIMD-accelerated memory copy using NEON on AArch64 (ARM64).
///
/// This function uses **NEON (Advanced SIMD)** to copy memory in **16-byte chunks**.
/// It offers significant performance benefits on ARM64-based processors.
///
/// # Safety
/// - Requires the `neon` feature to be **enabled at runtime**.
/// - **Caller must ensure** that `dst` and `src` have at least `len` bytes of valid memory.
///
/// # Parameters
/// - `dst`: Mutable destination slice.
/// - `src`: Source slice.
///
/// # Performance
/// - Processes **16 bytes at a time**.
/// - Falls back to a scalar copy for any remaining bytes.
#[inline]
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn simd_copy_arm(dst: &mut [u8], src: &[u8]) {
    let len = dst.len().min(src.len());
    let chunks = len / 16; // NEON processes 16 bytes at a time

    let mut i = 0;
    while i < chunks * 16 {
        let data = vld1q_u8(src.as_ptr().add(i));
        vst1q_u8(dst.as_mut_ptr().add(i), data);
        i += 16;
    }

    // Copy remaining bytes normally
    dst[i..len].copy_from_slice(&src[i..len]);
}

#[inline]
pub fn simd_copy(dst: &mut [u8], src: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") {
            unsafe {
                return simd_copy_x86(dst, src);
            }
        } else {
            // Note: This condition is met running Windows 11 Arm in UTM v. 4.4.5 on Mac
            // Log the warning only once
            LOG_ONCE.call_once(|| {
                warn!("Warning: AVX2 not detected, falling back to scalar copy.");
            });
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        // No standard runtime feature detection, use fallback by default
        unsafe {
            return simd_copy_arm(dst, src);
        }
    }

    // Fallback for unsupported architectures
    #[allow(unreachable_code)]
    dst.copy_from_slice(&src[..dst.len().min(src.len())]);
}
