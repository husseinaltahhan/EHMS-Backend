"""Alert logic: decides when vitals are bad enough to alert. No MQTT or web code here."""

# Normal ranges per age group. Placeholder values: confirm with clinical stakeholders.
THRESHOLDS = {
    "neonate": {"hr_low": 100, "hr_high": 180, "spo2_low": 90},
    "infant":  {"hr_low": 100, "hr_high": 160, "spo2_low": 92},
    "child":   {"hr_low": 70,  "hr_high": 140, "spo2_low": 94},
    "adult":   {"hr_low": 50,  "hr_high": 120, "spo2_low": 92},
    "elderly": {"hr_low": 50,  "hr_high": 110, "spo2_low": 90},
}


class Detector:
    def __init__(self, sustain_s=10, min_quality=0.5, hysteresis=0.05):
        self.sustain_s = sustain_s      # how long a breach must last before alerting
        self.min_quality = min_quality  # readings below this quality are ignored
        self.hysteresis = hysteresis    # how far back into normal a value must go to clear (5%)
        self.breach_since = {}          # (band_id, metric) -> time the breach started
        self.active = {}                # (band_id, metric) -> the active alert

    def process(self, sample, age_group="adult"):
        """Takes one reading (a dict), returns a list of alert events (usually empty)."""
        # Rule 1: ignore junk readings (band not on skin, or bad signal).
        if not sample.get("contact", True) or sample.get("quality", 1.0) < self.min_quality:
            return []

        t = THRESHOLDS.get(age_group, THRESHOLDS["adult"])
        h = self.hysteresis
        hr, spo2 = sample.get("hr"), sample.get("spo2")

        # Each check: (metric, value, is it bad?, has it clearly recovered?)
        checks = []
        if hr is not None:
            checks.append(("hr_high", hr, hr > t["hr_high"], hr < t["hr_high"] * (1 - h)))
            checks.append(("hr_low", hr, hr < t["hr_low"], hr > t["hr_low"] * (1 + h)))
        if spo2 is not None:
            checks.append(("spo2_low", spo2, spo2 < t["spo2_low"], spo2 > t["spo2_low"] * (1 + h)))

        events = []
        for metric, value, bad, recovered in checks:
            key = (sample["band_id"], metric)
            if key not in self.active:
                if bad:
                    self.breach_since.setdefault(key, sample["ts"])
                    # Rule 2: only alert once it's been bad for sustain_s seconds.
                    if sample["ts"] - self.breach_since[key] >= self.sustain_s:
                        alert = {"band_id": sample["band_id"], "metric": metric,
                                 "value": value, "raised_at": sample["ts"]}
                        self.active[key] = alert
                        events.append({"type": "raised", "alert": alert})
                else:
                    self.breach_since.pop(key, None)  # a single blip resets the clock
            elif recovered:
                # Rule 3 (hysteresis): only clear once clearly back in the normal range.
                alert = self.active.pop(key)
                self.breach_since.pop(key, None)
                events.append({"type": "cleared", "alert": alert})
        return events
