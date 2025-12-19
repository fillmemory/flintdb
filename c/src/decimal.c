#include "flintdb.h"
#include "runtime.h"
#include "allocator.h"
#include "simd.h"

#include <stdio.h>


// Build BCD MSB-first (digits are stored high-to-low, independent of binary data endianness)
// with target scale; truncates extra fraction; clamps to 16 bytes (32 digits)
int flintdb_decimal_from_string(const char *s, i16 scale, struct flintdb_decimal  *out) {
    if (!s || !out)
        return -1;
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r')
        s++;
    int neg = 0;
    if (*s == '+' || *s == '-') {
        neg = (*s == '-');
        s++;
    }
    // collect digits and track fractional digits
    const char *p = s;
    int dot = -1;
    int nd = 0;
    char digits[128];
    while (*p) {
        if (*p >= '0' && *p <= '9') {
            if (nd < (int)sizeof(digits))
                digits[nd++] = (char)(*p - '0');
        } else if (*p == '.' && dot < 0) {
            dot = nd;
        } else
            break;
        p++;
    }
    if (nd == 0) {
        memset(out, 0, sizeof(*out));
        return 0;
    }
    int frac = 0;
    if (dot >= 0)
        frac = nd - dot; // digits after dot
    // Adjust to target scale
    int target = (scale < 0) ? 0 : scale;
    int keep = nd; // digits to keep
    if (frac < target) {
        // need to append zeros
        int add = target - frac;
        if (nd + add > (int)sizeof(digits))
            add = (int)sizeof(digits) - nd;
        for (int i = 0; i < add; i++)
            digits[nd++] = 0;
        keep = nd;
    } else if (frac > target) {
        // truncate extra fractional digits
        keep = nd - (frac - target);
    }
    if (keep <= 0) {
        memset(out, 0, sizeof(*out));
        return 0;
    }
    // drop leading zeros to fit into 32 digits, but keep at least target scale zeros
    int lead = 0;
    while (lead < keep - 1 && digits[lead] == 0 && (keep - lead) > target + 1)
        lead++;
    int used = keep - lead;
    if (used < 1)
        used = 1;
    // Encode to standard BCD MSB-first (digit order is high-to-low, independent of binary endianness)
    // with EVEN number of nibbles to avoid ambiguous trailing nibble.
    // We LEFT-PAD with a zero nibble when the number of digits is odd.
    // Layout example:
    //   digits: [8,0,0] (3 digits, scale=2) => nibbles: [0,8, 0,0] => bytes: 0x08, 0x00
    //   digits: [3,6,.,0,0] (already even without dot) => nibbles: [3,6, 0,0] => bytes: 0x36, 0x00
    unsigned char outb[16] = {0};
    u32 outDigits = (u32)used;
    if (outDigits > 32)
        outDigits = 32;                                // clamp to 16 bytes
    int needPadNibble = (outDigits & 1) ? 1 : 0;       // 1 if odd
    u32 totalNibbles = outDigits + (u32)needPadNibble; // even number
    u32 outbytes = totalNibbles / 2;
    if (outbytes > 16)
        outbytes = 16;
    // Fill nibbles left-to-right, high then low for each byte
    u32 srcIdx = 0; // index into digits[lead + srcIdx]
    for (u32 nib = 0, byteIdx = 0; nib < totalNibbles && byteIdx < outbytes; nib++) {
        unsigned char val;
        if (needPadNibble && nib == 0) {
            // leading pad nibble = 0
            val = 0;
        } else {
            if (srcIdx >= outDigits)
                break;
            val = (unsigned char)(digits[lead + srcIdx] & 0x0F);
            srcIdx++;
        }
        if ((nib & 1) == 0) {
            // high nibble
            outb[byteIdx] = (unsigned char)(val << 4);
        } else {
            // low nibble, advance byte
            outb[byteIdx] |= val;
            byteIdx++;
        }
    }
    memset(out, 0, sizeof(*out));
    out->sign = neg ? 1 : 0;
    out->scale = (u8)target;
    out->raw = 0; // BCD encoded
    out->length = outbytes;
    simd_memcpy(out->data, outb, outbytes);
    return 0;
}

int flintdb_decimal_to_string(const struct flintdb_decimal  *d, char *buf, size_t buflen) {
    if (!d || !buf || buflen == 0)
        return -1;
    
    // Fast path: if raw=2, it's already a string
    if (d->raw == 2) {
        size_t len = d->length;
        if (len >= buflen) len = buflen - 1;
        simd_memcpy(buf, d->data, len);
        buf[len] = '\0';
        return 0;
    }
    
    // If raw=1, convert to BCD first
    struct flintdb_decimal bcd = {0};
    if (d->raw == 1) {
        // Convert two's-complement bytes to BCD
        // Reuse the conversion logic from row.c's decimal_from_twos_bytes
        if (d->length == 0) {
            bcd.scale = d->scale;
            bcd.sign = 0;
            bcd.raw = 0;
            bcd.length = 1;
            bcd.data[0] = 0;
        } else {
            // Use external helper or inline conversion
            // For now, use a simplified approach assuming we can call the function
            // This requires exposing decimal_from_twos_bytes or duplicating the logic
            // Since we can't easily call row.c functions from decimal.c, we'll need to handle this differently
            // Best approach: create a shared helper in a common file or expose via internal.h
            // For now, let's inline a minimal version
            
            // Determine sign from MSB (two's complement)
            const u8 *p = (const u8 *)d->data;
            u32 n = d->length;
            int neg = (p[n - 1] & 0x80) ? 1 : 0;
            
            u8 mag[16];
            simd_memcpy(mag, p, n);
            if (neg) {
                // two's complement inversion
                for (u32 i = 0; i < n; i++)
                    mag[i] = (u8)(~mag[i]);
                for (u32 i = 0; i < n; i++) {
                    unsigned int v = (unsigned int)mag[i] + 1u;
                    mag[i] = (u8)(v & 0xFFu);
                    if ((v & 0x100u) == 0)
                        break;
                }
            }
            
            // Convert to decimal digits via division by 10
            u8 rev[64];
            int nd = 0;
            u32 end = n;
            while (end > 1 && mag[end - 1] == 0)
                end--;
            
            if (end == 1 && mag[0] == 0) {
                rev[nd++] = 0;
            } else {
                u32 len = end;
                int nonzero = 1;
                while (nonzero && nd < (int)sizeof(rev)) {
                    // Divide by 10
                    unsigned int carry = 0;
                    for (int i = (int)len - 1; i >= 0; i--) {
                        unsigned int cur = (carry << 8) | mag[i];
                        mag[i] = (u8)(cur / 10);
                        carry = cur % 10;
                    }
                    rev[nd++] = (u8)carry;
                    while (len > 1 && mag[len - 1] == 0)
                        len--;
                    nonzero = !(len == 1 && mag[0] == 0);
                }
                if (nd == 0)
                    rev[nd++] = 0;
            }
            
            // Pack to BCD
            bcd.sign = neg ? 1 : 0;
            bcd.scale = d->scale;
            bcd.raw = 0;
            int bi = 0;
            int msd = nd - 1;
            int maxDigits = 32;
            if (nd > maxDigits)
                msd = maxDigits - 1;
            int used = (msd + 1);
            if ((used & 1) != 0) {
                u8 dgt = (msd >= 0) ? rev[msd--] : 0;
                bcd.data[bi++] = (u8)((0u << 4) | (dgt & 0x0F));
            }
            while (msd >= 0 && bi < (int)sizeof(bcd.data)) {
                u8 hi = rev[msd--] & 0x0F;
                u8 lo = (msd >= 0) ? (rev[msd--] & 0x0F) : 0;
                bcd.data[bi++] = (u8)((hi << 4) | lo);
            }
            bcd.length = (u32)bi;
        }
        d = &bcd; // Use converted BCD
    }
    
    // extract digits MSB-first from BCD (digit order is high-to-low, independent of binary endianness)
    int digits = (int)d->length * 2;
    // avoid buffer overflow; allocate temp digits
    int maxdigits = digits > 0 ? digits : 1;
    int *arr = (int *)MALLOC(sizeof(int) * (size_t)maxdigits);
    if (!arr)
        return -1;
    int idx = 0;
    for (u32 i = 0; i < d->length; i++) {
        unsigned char b = (unsigned char)d->data[i];
        int hi = (b >> 4) & 0x0F;
        int lo = b & 0x0F;
        arr[idx++] = hi;
        arr[idx++] = lo;
    }
    // remove leading zeros
    int start = 0;
    while (start < idx - 1 && arr[start] == 0)
        start++;
    i16 scale = d->scale;
    // build string
    size_t pos = 0;
    if (d->sign) {
        if (pos < buflen)
            buf[pos++] = '-';
    }
    int intDigits = (idx - start) - scale;
    if (intDigits <= 0) {
        if (pos < buflen)
            buf[pos++] = '0';
    } else {
        for (int i = 0; i < intDigits; i++) {
            int dig = arr[start + i];
            if (pos < buflen)
                buf[pos++] = (char)('0' + dig);
        }
    }
    if (scale > 0) {
        if (pos < buflen)
            buf[pos++] = '.';
        int fracStart = start + ((intDigits > 0) ? intDigits : 0);
        int fracDigits = idx - fracStart;
        // leading zeros in fraction if intDigits <= 0
        int needZeros = (intDigits < 0) ? (-intDigits) : 0;
        for (int z = 0; z < needZeros; z++) {
            if (pos < buflen)
                buf[pos++] = '0';
        }
        for (int i = 0; i < fracDigits; i++) {
            int dig = arr[fracStart + i];
            if (pos < buflen)
                buf[pos++] = (char)('0' + dig);
        }
    }
    if (pos >= buflen)
        pos = buflen - 1;
    buf[pos] = '\0';
    int rv = (int)pos;
    FREE(arr);
    return rv;
}

struct flintdb_decimal  flintdb_decimal_from_f64(f64 v, i16 scale, char **e) {
    struct flintdb_decimal  d = {0};
    char buf[64];
    if (e)
        *e = NULL;
    snprintf(buf, sizeof(buf), "%.*f", scale, v);
    if (flintdb_decimal_from_string(buf, scale, &d) < 0) {
        if (e)
            *e = "decimal_from_f64: failed to convert";
    }
    return d;
}

f64 flintdb_decimal_to_f64(const struct flintdb_decimal  *d, char **e) {
	if (e)
		*e = NULL;
	char buf[128];
	if (flintdb_decimal_to_string(d, buf, sizeof(buf)) < 0) {
		if (e)
			*e = "decimal_to_f64: failed to convert";
		return 0.0;
	}
	char *endptr = NULL;
	f64 val = strtod(buf, &endptr);
	if (endptr == buf) {
		if (e)
			*e = "decimal_to_f64: invalid conversion";
		return 0.0;
	}
	return val;
}

// Normalize decimal string representation to exact scale S.
// Input s may be like "-12.3" or "0"; output will be like "-12.30" if S=2.
static void normalize_decimal_string(const char *s, int S, char *out, size_t outsz, int *neg_out) {
	if (!s || !out || outsz == 0) return;
	if (neg_out) *neg_out = 0;
	const char *p = s;
	if (*p == '+') p++;
	else if (*p == '-') { if (neg_out) *neg_out = 1; p++; }
	// Split integer and fractional parts
	const char *dot = strchr(p, '.');
	size_t int_len = 0, frac_len = 0;
	const char *int_start = p;
	const char *frac_start = NULL;
	if (dot) {
		int_len = (size_t)(dot - p);
		frac_start = dot + 1;
		frac_len = strlen(frac_start);
	} else {
		int_len = strlen(p);
	}
	// Trim leading zeros in integer part but keep at least one digit
	while (int_len > 1 && *int_start == '0') { int_start++; int_len--; }

	char *w = out; size_t cap = outsz;
	size_t used = 0;
	if (neg_out && *neg_out) { if (used + 1 < cap) out[used++] = '-'; }
	// write integer part
	if (int_len == 0) { if (used + 1 < cap) out[used++] = '0'; }
	else {
		for (size_t i = 0; i < int_len && used + 1 < cap; i++) out[used++] = int_start[i];
	}
	if (S > 0) {
		if (used + 1 < cap) out[used++] = '.';
		// write/adjust fractional
		if (frac_len == 0) {
			// all zeros
			for (int i = 0; i < S && used + 1 < cap; i++) out[used++] = '0';
		} else {
			// copy min(frac_len, S), then pad zeros if needed
			size_t copy = (frac_len < (size_t)S) ? frac_len : (size_t)S;
			for (size_t i = 0; i < copy && used + 1 < cap; i++) out[used++] = frac_start[i];
			for (int i = (int)copy; i < S && used + 1 < cap; i++) out[used++] = '0';
		}
	}
	if (used >= cap) used = cap - 1;
	out[used] = '\0';
	(void)w;
}

// Remove dot and return only digits; return length written
static int strip_dot_digits(const char *s, char *out, size_t outsz) {
	size_t used = 0;
	for (const char *p = s; *p; ++p) {
		if (*p == '.') continue;
		if (*p == '-' || *p == '+') continue;
		if (*p < '0' || *p > '9') break;
		if (used + 1 < outsz) out[used++] = *p; else break;
	}
	if (used >= outsz) used = outsz - 1;
	if (out && outsz) out[used] = '\0';
	return (int)used;
}

static int cmp_abs_digits(const char *a, int la, const char *b, int lb) {
	if (la != lb) return (la < lb) ? -1 : 1;
	for (int i = 0; i < la; i++) {
		if (a[i] != b[i]) return (a[i] < b[i]) ? -1 : 1;
	}
	return 0;
}

static int add_abs_digits(const char *a, int la, const char *b, int lb, char *out, size_t outsz) {
	int ia = la - 1, ib = lb - 1; int carry = 0; int pos = 0;
	char tmp[128];
	while ((ia >= 0 || ib >= 0 || carry) && pos < (int)sizeof(tmp)) {
		int da = (ia >= 0) ? (a[ia--] - '0') : 0;
		int db = (ib >= 0) ? (b[ib--] - '0') : 0;
		int s = da + db + carry;
		tmp[pos++] = (char)('0' + (s % 10));
		carry = s / 10;
	}
	// reverse into out
	int outlen = pos;
	if ((size_t)outlen + 1 > outsz) outlen = (int)outsz - 1;
	for (int i = 0; i < outlen; i++) out[i] = tmp[pos - 1 - i];
	out[outlen] = '\0';
	return outlen;
}

// assuming a >= b in absolute
static int sub_abs_digits(const char *a, int la, const char *b, int lb, char *out, size_t outsz) {
	int ia = la - 1, ib = lb - 1; int borrow = 0; int pos = 0;
	char tmp[128];
	while (ia >= 0) {
		int da = a[ia--] - '0';
		int db = (ib >= 0) ? (b[ib--] - '0') : 0;
		int d = da - borrow - db;
		if (d < 0) { d += 10; borrow = 1; } else borrow = 0;
		tmp[pos++] = (char)('0' + d);
		if (pos >= (int)sizeof(tmp)) break;
	}
	// strip leading zeros in tmp (which is reversed)
	while (pos > 1 && tmp[pos - 1] == '0') pos--;
	int outlen = pos;
	if ((size_t)outlen + 1 > outsz) outlen = (int)outsz - 1;
	for (int i = 0; i < outlen; i++) out[i] = tmp[pos - 1 - i];
	out[outlen] = '\0';
	return outlen;
}

// multiply decimal digit string by small integer (0..9). returns length
static int mul_small_digits(const char *a, int la, int m, char *outb, size_t outsz) {
	if (m <= 0 || la <= 0) {
		if (outsz) { outb[0] = '0'; if (outsz > 1) outb[1] = '\0'; }
		return 1;
	}
	int carry = 0; char tmp[600]; int pos = 0;
	for (int i = la - 1; i >= 0; --i) {
		int v = (a[i] - '0') * m + carry;
		tmp[pos++] = (char)('0' + (v % 10));
		carry = v / 10;
		if (pos >= (int)sizeof(tmp)) break;
	}
	while (carry > 0 && pos < (int)sizeof(tmp)) { tmp[pos++] = (char)('0' + (carry % 10)); carry /= 10; }
	int outlen = pos;
	if ((size_t)outlen + 1 > outsz) outlen = (int)outsz - 1;
	for (int i = 0; i < outlen; i++) outb[i] = tmp[pos - 1 - i];
	outb[outlen] = '\0';
	// trim leading zeros
	int z = 0; while (z < outlen - 1 && outb[z] == '0') z++;
	if (z > 0) { memmove(outb, outb + z, (size_t)(outlen - z)); outlen -= z; outb[outlen] = '\0'; }
	return outlen;
}

// Core: add two decimals and produce decimal with given scale.
int flintdb_decimal_plus(const struct flintdb_decimal  *a, const struct flintdb_decimal  *b, i16 scale, struct flintdb_decimal  *out) {
	if (!a || !b || !out) return -1;
	// 1) Normalize both to desired scale S
	int S = (scale < 0) ? 0 : scale;
	char sa[96], sb[96]; int na = 0, nb = 0; int nag = 0, nbg = 0;
	sa[0] = sb[0] = '\0';
	flintdb_decimal_to_string(a, sa, sizeof(sa));
	flintdb_decimal_to_string(b, sb, sizeof(sb));
	char na_s[96], nb_s[96];
	normalize_decimal_string(sa, S, na_s, sizeof(na_s), &nag);
	normalize_decimal_string(sb, S, nb_s, sizeof(nb_s), &nbg);
	char a_digits[96], b_digits[96];
	na = strip_dot_digits(na_s, a_digits, sizeof(a_digits));
	nb = strip_dot_digits(nb_s, b_digits, sizeof(b_digits));
	if (na <= 0) { a_digits[0] = '0'; a_digits[1] = '\0'; na = 1; }
	if (nb <= 0) { b_digits[0] = '0'; b_digits[1] = '\0'; nb = 1; }

	// 2) Perform big integer add/sub based on signs
	char sum_digits[128]; int sum_len = 0; int neg = 0;
	if (nag == nbg) {
		neg = nag;
		sum_len = add_abs_digits(a_digits, na, b_digits, nb, sum_digits, sizeof(sum_digits));
	} else {
		int cmp = cmp_abs_digits(a_digits, na, b_digits, nb);
		if (cmp == 0) {
			// result is zero
			sum_digits[0] = '0'; sum_digits[1] = '\0'; sum_len = 1; neg = 0;
		} else if (cmp > 0) {
			// |a| > |b| => a - b, sign of a
			neg = nag;
			sum_len = sub_abs_digits(a_digits, na, b_digits, nb, sum_digits, sizeof(sum_digits));
		} else {
			// |b| > |a| => b - a, sign of b
			neg = nbg;
			sum_len = sub_abs_digits(b_digits, nb, a_digits, na, sum_digits, sizeof(sum_digits));
		}
	}

	// 3) Build string with decimal point at scale S
	char res_str[160]; size_t rp = 0; size_t cap = sizeof(res_str);
	if (neg && !(sum_len == 1 && sum_digits[0] == '0')) { if (rp + 1 < cap) res_str[rp++] = '-'; }
	if (S == 0) {
		for (int i = 0; i < sum_len && rp + 1 < cap; i++) res_str[rp++] = sum_digits[i];
	} else {
		if (sum_len <= S) {
			// 0.(zeros)digits
			if (rp + 1 < cap) res_str[rp++] = '0';
			if (rp + 1 < cap) res_str[rp++] = '.';
			int z = S - sum_len;
			for (int i = 0; i < z && rp + 1 < cap; i++) res_str[rp++] = '0';
			for (int i = 0; i < sum_len && rp + 1 < cap; i++) res_str[rp++] = sum_digits[i];
		} else {
			int intd = sum_len - S;
			for (int i = 0; i < intd && rp + 1 < cap; i++) res_str[rp++] = sum_digits[i];
			if (rp + 1 < cap) res_str[rp++] = '.';
			for (int i = intd; i < sum_len && rp + 1 < cap; i++) res_str[rp++] = sum_digits[i];
		}
	}
	if (rp >= cap) rp = cap - 1;
	res_str[rp] = '\0';

	// 4) Convert to struct flintdb_decimal  at exact scale S
	struct flintdb_decimal  d = {0};
	if (flintdb_decimal_from_string(res_str, S, &d) != 0) return -1;
	*out = d;
	return 0;
}


int flintdb_decimal_divide(const struct flintdb_decimal  *numerator, const struct flintdb_decimal  *denominator, i16 scale, struct flintdb_decimal  *out) {
	if (!numerator || !denominator || !out) return -1;

	// Build plain digit strings for numerator and denominator (no sign, no dot)
	char sn[96], sd[96];
	sn[0] = sd[0] = '\0';
	flintdb_decimal_to_string(numerator, sn, sizeof(sn));
	flintdb_decimal_to_string(denominator, sd, sizeof(sd));

	char n_digits[256], d_digits[256];
	int ln = strip_dot_digits(sn, n_digits, sizeof(n_digits));
	int ld = strip_dot_digits(sd, d_digits, sizeof(d_digits));
	if (ln <= 0) { // numerator is zero
		struct flintdb_decimal  zero = {0};
		zero.scale = (u8)((scale < 0) ? 0 : scale);
		*out = zero;
		return 0;
	}
	// check denominator zero
	int den_is_zero = 1;
	for (int i = 0; i < ld; i++) { if (d_digits[i] != '0') { den_is_zero = 0; break; } }
	if (ld <= 0 || den_is_zero) return -1;

	// Compute scaling factor: K = S + sD - sN
	int S = (scale < 0) ? 0 : scale;
	int sN = numerator->scale;
	int sD = denominator->scale;
	long K = (long)S + (long)sD - (long)sN;

	// Prepare scaled numerator and denominator (as digit strings)
	char num_scaled[512];
	char den_scaled[512];
	int lnum = ln;
	int lden = ld;
	// copy originals
	if ((size_t)ln >= sizeof(num_scaled)) lnum = (int)sizeof(num_scaled) - 1;
	memcpy(num_scaled, n_digits, (size_t)lnum);
	num_scaled[lnum] = '\0';
	if ((size_t)ld >= sizeof(den_scaled)) lden = (int)sizeof(den_scaled) - 1;
	memcpy(den_scaled, d_digits, (size_t)lden);
	den_scaled[lden] = '\0';

	if (K > 0) {
		// append K zeros to numerator
		long append = K;
		if ((size_t)lnum + (size_t)append >= sizeof(num_scaled)) {
			append = (long)sizeof(num_scaled) - 1 - lnum;
		}

		// Trim leading zeros in denominator digits to avoid inflated length
		int dlead = 0;
		while (dlead < lden - 1 && den_scaled[dlead] == '0') dlead++;
		if (dlead > 0) { memmove(den_scaled, den_scaled + dlead, (size_t)(lden - dlead)); lden -= dlead; den_scaled[lden] = '\0'; }
		// If denominator is still zero after trim, error
		den_is_zero = 1;
		for (int i = 0; i < lden; i++) { if (den_scaled[i] != '0') { den_is_zero = 0; break; } }
		if (lden <= 0 || den_is_zero) return -1;
		for (long i = 0; i < append; i++) num_scaled[lnum++] = '0';
		num_scaled[lnum] = '\0';
	} else if (K < 0) {
		// multiply denominator by 10^{-K} => append zeros to denominator
		long append = -K;
		if ((size_t)lden + (size_t)append >= sizeof(den_scaled)) {
			append = (long)sizeof(den_scaled) - 1 - lden;
		}
		for (long i = 0; i < append; i++) den_scaled[lden++] = '0';
		den_scaled[lden] = '\0';
	}

	// Long division: quotient = floor(num_scaled / den_scaled)
	char rem[600]; int lrem = 0; rem[0] = '\0';
	char qbuf[700]; int qlen = 0;
	for (int i = 0; i < lnum; i++) {
		// append next digit to remainder
		if (lrem == 1 && rem[0] == '0') lrem = 0; // normalize zero
		if (lrem + 1 < (int)sizeof(rem)) {
			rem[lrem++] = num_scaled[i];
			rem[lrem] = '\0';
		}
		// trim leading zeros in remainder
		int z = 0; while (z < lrem - 1 && rem[z] == '0') z++;
		if (z > 0) { memmove(rem, rem + z, (size_t)(lrem - z)); lrem -= z; rem[lrem] = '\0'; }

		// determine next quotient digit
		int qd = 0;
		// quick compare if remainder < den then qd stays 0
		int cmp = cmp_abs_digits(rem, lrem, den_scaled, lden);
		if (cmp >= 0) {
			// find qd in [1..9]
			for (int d = 9; d >= 1; --d) {
				char prod[600]; int lp = mul_small_digits(den_scaled, lden, d, prod, sizeof(prod));
				int c = cmp_abs_digits(prod, lp, rem, lrem);
				if (c <= 0) { qd = d; // remainder >= den*d
					// remainder = remainder - prod
					char newrem[600]; int newlen = sub_abs_digits(rem, lrem, prod, lp, newrem, sizeof(newrem));
					if (newlen < 0) newlen = 0;
					if ((size_t)newlen >= sizeof(rem)) newlen = (int)sizeof(rem) - 1;
					memcpy(rem, newrem, (size_t)newlen);
					lrem = newlen; rem[lrem] = '\0';
					break;
				}
			}
		}
		if (qlen + 1 < (int)sizeof(qbuf)) qbuf[qlen++] = (char)('0' + qd);
	}
	if (qlen == 0) { qbuf[qlen++] = '0'; }
	// strip leading zeros in quotient
	int qz = 0; while (qz < qlen - 1 && qbuf[qz] == '0') qz++;
	if (qz > 0) { memmove(qbuf, qbuf + qz, (size_t)(qlen - qz)); qlen -= qz; }
	qbuf[qlen] = '\0';

	// Build result string with target scale S
	int neg = (numerator->sign ^ denominator->sign) ? 1 : 0;
	// if numerator is zero, force non-negative
	int num_is_zero = 1; for (int i = 0; i < ln; i++) { if (n_digits[i] != '0') { num_is_zero = 0; break; } }
	if (num_is_zero) neg = 0;

	char res_str[900]; size_t rp = 0; size_t cap = sizeof(res_str);
	if (neg) { if (rp + 1 < cap) res_str[rp++] = '-'; }
	if (S == 0) {
		for (int i = 0; i < qlen && rp + 1 < cap; i++) res_str[rp++] = qbuf[i];
	} else {
		if (qlen <= S) {
			if (rp + 1 < cap) res_str[rp++] = '0';
			if (rp + 1 < cap) res_str[rp++] = '.';
			int pad = S - qlen;
			for (int i = 0; i < pad && rp + 1 < cap; i++) res_str[rp++] = '0';
			for (int i = 0; i < qlen && rp + 1 < cap; i++) res_str[rp++] = qbuf[i];
		} else {
			int intd = qlen - S;
			for (int i = 0; i < intd && rp + 1 < cap; i++) res_str[rp++] = qbuf[i];
			if (rp + 1 < cap) res_str[rp++] = '.';
			for (int i = intd; i < qlen && rp + 1 < cap; i++) res_str[rp++] = qbuf[i];
		}
	}
	if (rp >= cap) rp = cap - 1;
	res_str[rp] = '\0';

	struct flintdb_decimal  d = {0};
	if (flintdb_decimal_from_string(res_str, S, &d) != 0) return -1;
	*out = d;
	return 0;
}

int flintdb_decimal_divide_by_int(const struct flintdb_decimal  *numerator, int denominator, struct flintdb_decimal  *out) {
	if (!numerator || !out) return -1;
	if (denominator == 0) return -1;
	char buf[64];
	snprintf(buf, sizeof(buf), "%d", denominator);
	struct flintdb_decimal  den = {0};
	if (flintdb_decimal_from_string(buf, 0, &den) != 0) return -1;
	return flintdb_decimal_divide(numerator, &den, numerator->scale, out);
}
