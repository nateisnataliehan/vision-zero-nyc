"""
Vision Zero NYC — within-NYC pedestrian fatalities animation.
Within-NYC pre/post analysis only (no cross-city DiD).
Numbers come from the VisionZeroAnalytic Spark job's annual_summary output.
Produces vision_zero.gif and vision_zero.mp4.
"""

import matplotlib.pyplot as plt
import matplotlib.animation as animation
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import numpy as np

years = list(range(2013, 2026))
ped_killed = [176, 133, 133, 149, 127, 123, 131, 101, 132, 135, 107, 124, 119]
total_killed = [297, 262, 243, 246, 256, 231, 244, 269, 297, 290, 280, 268, 227]

VZ_YEAR = 2014

pre_ped_avg = ped_killed[0]
post_ped_avg = sum(ped_killed[1:]) / len(ped_killed[1:])
ped_drop_pct = (pre_ped_avg - post_ped_avg) / pre_ped_avg * 100

fig, ax = plt.subplots(figsize=(10, 6), dpi=120)
fig.patch.set_facecolor("#0e1117")
ax.set_facecolor("#0e1117")

for spine in ax.spines.values():
    spine.set_color("#444")
ax.tick_params(colors="#cfcfcf")
ax.title.set_color("white")
ax.xaxis.label.set_color("#cfcfcf")
ax.yaxis.label.set_color("#cfcfcf")

ax.set_xlim(2012.5, 2025.5)
ax.set_ylim(0, 320)
ax.set_xlabel("Year")
ax.set_ylabel("Persons killed in NYC traffic crashes")
ax.set_title("Within-NYC Vision Zero Analysis — Persons Killed, 2013–2025",
             fontsize=14, weight="bold")
ax.grid(True, alpha=0.15)

ped_line, = ax.plot([], [], color="#ff7a59", lw=2.5, marker="o", ms=5, label="Pedestrians killed")
tot_line, = ax.plot([], [], color="#5cc8ff", lw=2.5, marker="s", ms=5, label="All persons killed")

vz_line = ax.axvline(VZ_YEAR, color="#ffd166", lw=2, ls="--", alpha=0)
vz_text = ax.text(VZ_YEAR + 0.08, 305, "Vision Zero launched",
                  color="#ffd166", fontsize=11, weight="bold", alpha=0)

callout = ax.text(0.5, 0.5, "", transform=ax.transAxes,
                  ha="center", va="center", color="white",
                  fontsize=22, weight="bold", alpha=0,
                  bbox=dict(boxstyle="round,pad=0.6", fc="#1f6feb", ec="none", alpha=0.0))

legend = ax.legend(loc="upper right", facecolor="#1a1d24", edgecolor="#444", labelcolor="white")

n = len(years)
intro_frames = 15
draw_frames = n
vz_appear_frame = intro_frames + (VZ_YEAR - years[0])
callout_start = intro_frames + draw_frames + 8
callout_end = callout_start + 30
total_frames = callout_end + 20


def init():
    ped_line.set_data([], [])
    tot_line.set_data([], [])
    vz_line.set_alpha(0)
    vz_text.set_alpha(0)
    callout.set_alpha(0)
    return ped_line, tot_line, vz_line, vz_text, callout


def update(frame):
    if frame < intro_frames:
        return ped_line, tot_line, vz_line, vz_text, callout

    drawn = min(frame - intro_frames + 1, n)
    ped_line.set_data(years[:drawn], ped_killed[:drawn])
    tot_line.set_data(years[:drawn], total_killed[:drawn])

    if frame >= vz_appear_frame:
        a = min(1.0, (frame - vz_appear_frame) / 6.0)
        vz_line.set_alpha(a)
        vz_text.set_alpha(a)

    if frame >= callout_start:
        a = min(1.0, (frame - callout_start) / 10.0)
        callout.set_text(
            f"Pedestrians killed per year\n"
            f"pre-2014: {pre_ped_avg:.0f}   post-2014 avg: {post_ped_avg:.0f}\n"
            f"−{ped_drop_pct:.0f}% within-NYC"
        )
        callout.set_alpha(a)
        callout.get_bbox_patch().set_alpha(0.85 * a)

    return ped_line, tot_line, vz_line, vz_text, callout


anim = animation.FuncAnimation(
    fig, update, init_func=init,
    frames=total_frames, interval=120, blit=False, repeat=False
)

print("Writing GIF...")
anim.save("vision_zero.gif", writer=animation.PillowWriter(fps=8))
print("GIF done.")

try:
    print("Writing MP4...")
    anim.save("vision_zero.mp4", writer=animation.FFMpegWriter(fps=8, bitrate=2000))
    print("MP4 done.")
except Exception as e:
    print(f"MP4 skipped (ffmpeg not installed): {e}")

plt.close(fig)
print("Saved vision_zero.gif (and .mp4 if ffmpeg present).")
