from detector import Detector


def reading(ts, hr=80, spo2=97, quality=0.9, contact=True):
    return {"band_id": "band1", "ts": ts, "hr": hr, "spo2": spo2, "quality": quality, "contact": contact}


def run(detector, readings, age_group="adult"):
    events = []
    for r in readings:
        events += detector.process(r, age_group)
    return [e["type"] for e in events]


def test_normal_vitals_no_alert():
    assert run(Detector(), [reading(t) for t in range(30)]) == []


def test_sustained_high_hr_alerts_once():
    assert run(Detector(sustain_s=10), [reading(t, hr=150) for t in range(30)]) == ["raised"]


def test_single_blip_does_not_alert():
    readings = [reading(t) for t in range(10)] + [reading(10, hr=150)] + [reading(t) for t in range(11, 30)]
    assert run(Detector(sustain_s=10), readings) == []


def test_short_breach_does_not_alert():
    assert run(Detector(sustain_s=10), [reading(t, spo2=85) for t in range(8)]) == []


def test_hovering_near_threshold_does_not_flicker():
    high = [reading(t, hr=130) for t in range(10)]          # adult limit is 120
    hovering = [reading(t, hr=118) for t in range(10, 20)]  # under 120 but not clearly normal (< 114)
    assert run(Detector(sustain_s=5), high + hovering) == ["raised"]


def test_clears_when_back_to_normal():
    readings = [reading(t, hr=130) for t in range(10)] + [reading(t, hr=80) for t in range(10, 15)]
    assert run(Detector(sustain_s=5), readings) == ["raised", "cleared"]


def test_junk_readings_ignored():
    readings = [reading(t, hr=200, quality=0.1) for t in range(30)] + [reading(t, hr=200, contact=False) for t in range(30, 60)]
    assert run(Detector(sustain_s=5), readings) == []


def test_age_groups_use_different_limits():
    readings = [reading(t, hr=130) for t in range(15)]
    assert run(Detector(sustain_s=5), readings, "adult") == ["raised"]
    assert run(Detector(sustain_s=5), readings, "child") == []
