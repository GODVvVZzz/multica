//go:build windows

package util

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// absNoClean makes p absolute the way the Windows loader would resolve it,
// without cleaning, so any ".." survives for evalPath to apply after following
// links. Windows has several relative kinds and they do NOT all resolve
// against the working directory, so they are preserved rather than prefixed:
//
//   - drive-absolute `C:\x` and UNC `\\srv\share\x` are already absolute.
//   - root-relative `\x` (or `/x`) resolves against the ROOT of the current
//     directory's volume — for a UNC current directory, against the share.
//     Prefixing the working directory instead would let `C:\task\workdir\tmp`
//     shadow the `C:\tmp` that os.ReadFile actually opens, and a containment
//     check would pass on the shadow while the kernel reads past it.
//   - drive-relative `C:x` resolves against the current directory ON C:. Only
//     the current drive's directory is observable to this process — it IS
//     os.Getwd() — so for that drive the prefix is dropped and the working
//     directory substituted. Any other drive's per-drive directory cannot be
//     read, so the path is handed back unresolved and fails closed downstream:
//     ResolveSymlinksBestEffort returns it unchanged and filepath.Rel refuses
//     to relate it to a workdir on another volume.
func absNoClean(p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	cwd, err := os.Getwd()
	if err != nil {
		return p
	}
	sep := string(filepath.Separator)
	if len(p) > 0 && os.IsPathSeparator(p[0]) {
		return filepath.VolumeName(cwd) + filepath.FromSlash(p)
	}
	if vol := filepath.VolumeName(p); vol != "" {
		if strings.EqualFold(vol, filepath.VolumeName(cwd)) {
			return strings.TrimSuffix(cwd, sep) + sep + p[len(vol):]
		}
		return p
	}
	return strings.TrimSuffix(cwd, sep) + sep + p
}

// maxFollowedLinks bounds how many reparse points one resolution may follow —
// the budget filepath.EvalSymlinks works with on Unix — so a junction chain
// that closes on itself turns into an error rather than a hang.
const maxFollowedLinks = 255

var errTooManyLinks = errors.New("too many levels of symbolic links")

// evalPath resolves p the way the kernel opens it, following both NTFS
// symlinks and directory junctions. filepath.EvalSymlinks cannot serve here:
// under the winsymlink semantics this module's go directive selects (Go 1.23+)
// it refuses to descend through a junction — it resolves the junction to its
// own name — which is what let an out-of-workdir junction read as inside the
// workdir. os.Readlink answers on a junction under both winsymlink settings,
// with a clean absolute target and no \??\ prefix (measured on 10.0.19045 /
// go1.26.6), and it answers even when the target no longer exists.
//
// Components are walked one at a time so the first missing or unreadable
// component is the error ResolveSymlinksBestEffort's walk switches to its
// lexical tail on, and ".." is applied to the resolved prefix, which is the
// order the kernel uses.
func evalPath(p string) (string, error) {
	root, segs := splitNoClean(p)
	resolved := root
	follows := 0
	for i := 0; i < len(segs); {
		seg := segs[i]
		i++
		switch seg {
		case "..":
			resolved = dropLastSegment(resolved)
			continue
		case ".", "":
			continue
		}
		cand := joinSegment(resolved, seg)
		fi, err := os.Lstat(cand)
		if err != nil {
			return "", err
		}
		link := fi.Mode()&os.ModeSymlink != 0
		irregular := fi.Mode()&os.ModeIrregular != 0
		if !link && !irregular {
			resolved = cand
			continue
		}
		target, err := os.Readlink(cand)
		if err != nil {
			if irregular {
				// A reparse point os.Readlink cannot speak for — a OneDrive
				// placeholder, a deduplicated file — is not a path redirect:
				// the kernel opens the component where it stands. Treat it as
				// an opaque regular component instead of failing the walk.
				resolved = cand
				continue
			}
			return "", err
		}
		follows++
		if follows > maxFollowedLinks {
			return "", errTooManyLinks
		}
		if trimmed, ok := trimDeviceNamespace(target); ok {
			target = trimmed
		}
		// Splice the target's components ahead of the remaining ones and keep
		// walking: a target can itself contain links, and its missing tail is
		// resolved in the same pass instead of being mistaken for a missing
		// component of the original path.
		tRoot, tSegs := splitNoClean(target)
		if filepath.IsAbs(target) {
			resolved = tRoot
		}
		// A relative target resolves against the directory containing the
		// link — the resolved prefix so far. Junctions always carry an
		// absolute target; this shape is NTFS symlinks.
		segs = append(tSegs, segs[i:]...)
		i = 0
	}
	return resolved, nil
}

// trimDeviceNamespace strips the NT device namespace prefixes a link target
// can carry. Junctions created by mklink /J answer os.Readlink with a bare
// Win32 path (measured), so this is defense in depth rather than the supported
// shape; a `\\?\UNC\` target becomes its Win32 `\\srv\share` form, which is
// the only spelling a containment comparison can relate to a workdir.
func trimDeviceNamespace(target string) (string, bool) {
	for _, prefix := range []string{`\\??\`, `\??\`, `\\?\`} {
		if strings.HasPrefix(target, prefix) {
			t := target[len(prefix):]
			if rest, found := strings.CutPrefix(t, `UNC\`); found {
				return `\\` + rest, true
			}
			return t, true
		}
	}
	return target, false
}
