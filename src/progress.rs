use indicatif::{ProgressBar, ProgressStyle};

#[inline]
pub fn styled_bar(count: u64) -> ProgressBar {
    let bar = indicatif::ProgressBar::new(count);
    bar.set_style(
        ProgressStyle::default_bar()
            .template("{spinner} [{bar:50}] {pos}/{len} • {eta_precise} remaining")
            .unwrap()
            .progress_chars("#>-"),
    );
    bar
}
