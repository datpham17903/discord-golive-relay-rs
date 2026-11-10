//! Port of `tests/control/test_cooldown.py`. The injected clock becomes an explicit `now`.

use relay_core::control::cooldown::ChatCooldowns;

#[test]
fn author_cooldown_notifies_once_then_silent() {
    let mut t = 1000.0;
    let mut cd = ChatCooldowns::new(5.0, 3.0);
    assert_eq!(cd.author(1, t), None);
    t += 1.2;
    assert_eq!(cd.author(1, t), Some(4)); // one notice with the seconds left
    t += 1.0;
    assert_eq!(cd.author(1, t), Some(0)); // silent for the rest of the window
    assert_eq!(cd.author(2, t), None); // other authors are independent
    t += 3.0;
    assert_eq!(cd.author(1, t), None); // window over: allowed and a new window starts
    assert_eq!(cd.author(1, t), Some(5));
}

#[test]
fn author_entries_are_pruned_after_the_cooldown() {
    let mut t = 1000.0;
    let mut cd = ChatCooldowns::new(5.0, 3.0);
    for a in 0..100 {
        cd.author(a, t);
    }
    t += 5.0;
    cd.author(1000, t);
    assert_eq!(cd.tracked_authors(), 1);
}

#[test]
fn change_cooldown_is_global() {
    let mut t = 1000.0;
    let mut cd = ChatCooldowns::new(5.0, 3.0);
    assert_eq!(cd.change_wait(t), 0);
    cd.changed(t);
    t += 0.5;
    assert_eq!(cd.change_wait(t), 3);
    t += 2.0;
    assert_eq!(cd.change_wait(t), 1);
    t += 0.5;
    assert_eq!(cd.change_wait(t), 0);
}

#[test]
fn zero_disables_cooldowns() {
    let t = 1000.0;
    let mut cd = ChatCooldowns::new(0.0, 0.0);
    assert!(cd.author(1, t).is_none() && cd.author(1, t).is_none());
    cd.changed(t);
    assert_eq!(cd.change_wait(t), 0);
}

#[test]
fn change_notice_once_per_author_per_window() {
    let mut cd = ChatCooldowns::new(5.0, 3.0);
    assert!(cd.change_notice(1));
    assert!(!cd.change_notice(1));
    assert!(cd.change_notice(2));
    cd.changed(0.0);
    assert!(cd.change_notice(1));
}
