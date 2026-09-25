package main

// Public matchmaking keeps a fixed, mode-specific target. Age-based threshold relaxation used to
// form partial rosters (for example, four or six players) while advertising an 8/8 session. That
// sent Splatoon into matches with missing players and caused the lobby to unwind. Players now stay
// in their requested mode queue until the full roster is available.

import "time"

// attenteLaPlusLongueLocked is retained for queue diagnostics. It never changes the match target.
// The caller holds m.mu.
func (m *matchmakerServer) attenteLaPlusLongueLocked(cfg string) time.Duration {
	var pire time.Duration
	maintenant := time.Now()
	for _, w := range m.waitersForConfigLocked(cfg) {
		if w.depuis.IsZero() {
			continue
		}
		if d := maintenant.Sub(w.depuis); d > pire {
			pire = d
		}
	}
	return pire
}

// seuilCourantLocked always returns the nominal target. Time spent queued must never shrink a
// regular-battle roster or make a smaller session look full.
func (m *matchmakerServer) seuilCourantLocked(cfg string, nominal int32) (int32, time.Duration) {
	return nominal, m.attenteLaPlusLongueLocked(cfg)
}
