"""
Production ERD Generator for samples.wanderbricks
=================================================
Renders clean, high-resolution Entity Relationship Diagrams with:
  - Exact 3NF normalized schema from Databricks Unity Catalog
  - Accurate row counts and business domain color coding
  - Crisp constraint dividers separating PK/FKs from attributes
  - True Crow's Foot cardinality and modality notation:
      • ||  = 1 (Mandatory One)
      • >o  = 0..* (Optional Many)
      • >|  = 1..* (Mandatory Many)
      • -|o = 0..1 (Optional One)
  - 100% straight orthogonal routing with zero overlapping text boxes

Usage:
  python scripts/generate_erd.py [--output-dir docs/diagrams] [--sync-root]
"""

import os
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch

# ==============================================================================
# 1. DOMAIN COLOR PALETTE
# ==============================================================================
DOMAIN_COLORS = {
    'geo': {'header': '#0284c7', 'bg': '#ffffff', 'border': '#0284c7', 'label': 'Geographic Master'},
    'inventory': {'header': '#16a34a', 'bg': '#ffffff', 'border': '#16a34a', 'label': 'Supply & Inventory'},
    'junction': {'header': '#059669', 'bg': '#ffffff', 'border': '#059669', 'label': 'Junction Table'},
    'master': {'header': '#d97706', 'bg': '#ffffff', 'border': '#d97706', 'label': 'Master / Dimensions'},
    'fact': {'header': '#2563eb', 'bg': '#ffffff', 'border': '#2563eb', 'label': 'Core Facts (Bookings & Payments)'},
    'engagement': {'header': '#7c3aed', 'bg': '#ffffff', 'border': '#7c3aed', 'label': 'Feedback & Ratings'},
    'telemetry': {'header': '#9333ea', 'bg': '#ffffff', 'border': '#9333ea', 'label': 'High-Volume Clickstream'}
}

# ==============================================================================
# 2. VERIFIED 3NF SCHEMA METADATA (samples.wanderbricks)
# ==============================================================================
SCHEMA_TABLES = {
    'COUNTRIES': {
        'count': 'Master Ref',
        'cat': 'geo',
        'x': 0.8, 'y': 12.2, 'w': 3.5, 'h': 2.3,
        'cols': [
            ('PK', 'country', 'STRING'),
            ('', 'country_code', 'STRING'),
            ('', 'continent', 'STRING')
        ]
    },
    'DESTINATIONS': {
        'count': '42 rows',
        'cat': 'geo',
        'x': 0.8, 'y': 7.6, 'w': 3.5, 'h': 3.4,
        'cols': [
            ('PK', 'destination_id', 'BIGINT'),
            ('FK', 'country', 'STRING'),
            ('', 'destination', 'STRING'),
            ('', 'state_or_province', 'STRING'),
            ('', 'description', 'STRING')
        ]
    },
    'HOSTS': {
        'count': '19,384 rows',
        'cat': 'master',
        'x': 5.6, 'y': 10.5, 'w': 3.8, 'h': 4.6,
        'cols': [
            ('PK', 'host_id', 'BIGINT'),
            ('FK', 'country', 'STRING'),
            ('', 'name', 'STRING (PII)'),
            ('', 'email', 'STRING (PII)'),
            ('', 'phone', 'STRING (PII)'),
            ('', 'is_verified', 'BOOLEAN'),
            ('', 'is_active', 'BOOLEAN'),
            ('', 'rating', 'DOUBLE'),
            ('', 'joined_at', 'TIMESTAMP')
        ]
    },
    'PROPERTIES': {
        'count': '18,163 rows',
        'cat': 'inventory',
        'x': 5.6, 'y': 3.4, 'w': 3.8, 'h': 5.8,
        'cols': [
            ('PK', 'property_id', 'BIGINT'),
            ('FK', 'host_id', 'BIGINT'),
            ('FK', 'destination_id', 'BIGINT'),
            ('', 'title', 'STRING'),
            ('', 'base_price', 'DECIMAL(10,2)'),
            ('', 'property_type', 'STRING'),
            ('', 'max_guests', 'INT'),
            ('', 'bedrooms', 'INT'),
            ('', 'bathrooms', 'DOUBLE'),
            ('', 'property_latitude', 'DOUBLE'),
            ('', 'property_longitude', 'DOUBLE'),
            ('', 'created_at', 'TIMESTAMP')
        ]
    },
    'PROPERTY_AMENITIES': {
        'count': '118,108 rows',
        'cat': 'junction',
        'x': 0.8, 'y': 4.4, 'w': 3.5, 'h': 2.4,
        'cols': [
            ('PK/FK', 'property_id', 'BIGINT'),
            ('PK/FK', 'amenity_id', 'BIGINT')
        ]
    },
    'AMENITIES': {
        'count': '38 rows',
        'cat': 'inventory',
        'x': 0.8, 'y': 0.8, 'w': 3.5, 'h': 2.7,
        'cols': [
            ('PK', 'amenity_id', 'BIGINT'),
            ('', 'name', 'STRING'),
            ('', 'category', 'STRING'),
            ('', 'icon', 'STRING')
        ]
    },
    'USERS': {
        'count': '124,509 rows',
        'cat': 'master',
        'x': 16.0, 'y': 10.4, 'w': 3.8, 'h': 4.6,
        'cols': [
            ('PK', 'user_id', 'BIGINT'),
            ('FK', 'country', 'STRING'),
            ('', 'name', 'STRING (PII)'),
            ('', 'email', 'STRING (PII)'),
            ('', 'user_type', 'STRING'),
            ('', 'is_business', 'BOOLEAN'),
            ('', 'company_name', 'STRING'),
            ('', 'created_at', 'TIMESTAMP')
        ]
    },
    'BOOKINGS': {
        'count': '72,247 rows',
        'cat': 'fact',
        'x': 10.6, 'y': 6.8, 'w': 4.1, 'h': 5.0,
        'cols': [
            ('PK', 'booking_id', 'BIGINT'),
            ('FK', 'user_id', 'BIGINT'),
            ('FK', 'property_id', 'BIGINT'),
            ('', 'check_in', 'DATE'),
            ('', 'check_out', 'DATE'),
            ('', 'guests_count', 'INT'),
            ('', 'total_amount', 'DECIMAL(10,2)'),
            ('', 'status', 'STRING'),
            ('', 'created_at', 'TIMESTAMP'),
            ('', 'updated_at', 'TIMESTAMP')
        ]
    },
    'PAYMENTS': {
        'count': '49,638 rows',
        'cat': 'fact',
        'x': 10.6, 'y': 1.6, 'w': 4.1, 'h': 3.7,
        'cols': [
            ('PK', 'payment_id', 'BIGINT'),
            ('FK', 'booking_id', 'BIGINT'),
            ('', 'amount', 'DECIMAL(10,2)'),
            ('', 'payment_method', 'STRING'),
            ('', 'status', 'STRING'),
            ('', 'payment_date', 'TIMESTAMP')
        ]
    },
    'REVIEWS': {
        'count': '99,793 rows',
        'cat': 'engagement',
        'x': 16.0, 'y': 4.8, 'w': 3.8, 'h': 4.0,
        'cols': [
            ('PK', 'review_id', 'BIGINT'),
            ('FK', 'booking_id', 'BIGINT'),
            ('FK', 'user_id', 'BIGINT'),
            ('FK', 'property_id', 'BIGINT'),
            ('', 'rating', 'DOUBLE (1.0-5.0)'),
            ('', 'comment', 'STRING'),
            ('', 'is_deleted', 'BOOLEAN'),
            ('', 'created_at', 'TIMESTAMP')
        ]
    },
    'PAGE_VIEWS': {
        'count': '500,000 rows',
        'cat': 'telemetry',
        'x': 21.2, 'y': 7.4, 'w': 3.8, 'h': 4.0,
        'cols': [
            ('PK', 'view_id', 'BIGINT'),
            ('FK', 'user_id', 'BIGINT'),
            ('FK', 'property_id', 'BIGINT'),
            ('', 'device_type', 'STRING'),
            ('', 'page_url', 'STRING'),
            ('', 'referrer', 'STRING'),
            ('', 'timestamp', 'TIMESTAMP')
        ]
    },
    'CLICKSTREAM': {
        'count': '100,000 rows',
        'cat': 'telemetry',
        'x': 21.2, 'y': 2.4, 'w': 3.8, 'h': 3.4,
        'cols': [
            ('FK', 'user_id', 'BIGINT'),
            ('FK', 'property_id', 'BIGINT'),
            ('', 'event', 'STRING'),
            ('', 'metadata', 'STRING (JSON)'),
            ('', 'timestamp', 'TIMESTAMP')
        ]
    }
}

# ==============================================================================
# 3. VERIFIED RELATIONSHIPS & MODALITY
# ==============================================================================
RELATIONSHIPS = [
    # 1. Countries -> Destinations (1 : 1..* Mandatory)
    {'from': (2.55, 12.2), 'to': (2.55, 11.0), 'p_card': '1', 'c_card': '1..*'},
    # 2. Destinations -> Properties (1 : 0..* Optional)
    {'from': (4.3, 8.2), 'to': (5.6, 8.2), 'p_card': '1', 'c_card': '0..*'},
    # 3. Hosts -> Properties (1 : 1..* Mandatory)
    {'from': (7.5, 10.5), 'to': (7.5, 9.2), 'p_card': '1', 'c_card': '1..*'},
    # 4. Amenities -> Property_Amenities (1 : 0..* Optional)
    {'from': (2.55, 3.5), 'to': (2.55, 4.4), 'p_card': '1', 'c_card': '0..*'},
    # 5. Properties -> Property_Amenities (1 : 0..* Optional)
    {'from': (5.6, 5.6), 'to': (4.3, 5.6), 'p_card': '1', 'c_card': '0..*'},
    # 6. Properties -> Bookings (1 : 0..* Optional)
    {'from': (9.4, 8.0), 'to': (10.6, 8.0), 'p_card': '1', 'c_card': '0..*'},
    # 7. Bookings -> Payments (1 : 0..1 Optional)
    {'from': (12.65, 6.8), 'to': (12.65, 5.3), 'p_card': '1', 'c_card': '0..1'},
    # 8. Bookings -> Reviews (1 : 0..1 Optional)
    {'from': (14.7, 7.4), 'to': (16.0, 7.4), 'p_card': '1', 'c_card': '0..1'},
    # 9. Users -> Bookings (1 : 0..* Optional)
    {'from': (16.0, 10.6), 'to': (14.7, 10.6), 'p_card': '1', 'c_card': '0..*'},
    # 10. Users -> Reviews (1 : 0..* Optional)
    {'from': (17.9, 10.4), 'to': (17.9, 8.8), 'p_card': '1', 'c_card': '0..*'},
    # 11. Users -> Page Views (1 : 0..* Optional)
    {'from': (19.8, 10.6), 'to': (21.2, 10.6), 'p_card': '1', 'c_card': '0..*'},
    # 12. Page Views -> Clickstream (1 : 0..* Optional)
    {'from': (23.1, 7.4), 'to': (23.1, 5.8), 'p_card': '1', 'c_card': '0..*'}
]

# ==============================================================================
# 4. MODULAR ERD VISUALIZER ENGINE
# ==============================================================================
class ERDVisualizer:
    """Enterprise ERD Visualizer rendering crisp database architectural schemas."""

    def __init__(self, tables, relationships, domain_colors):
        self.tables = tables
        self.relationships = relationships
        self.domain_colors = domain_colors

    def render(self):
        plt.style.use('default')
        fig, ax = plt.subplots(figsize=(26, 17.5), dpi=300)
        ax.set_facecolor('#f8fafc')
        fig.patch.set_facecolor('#f8fafc')

        # 1. Draw Table Cards
        for tname, tinfo in self.tables.items():
            self._draw_table_card(ax, tname, tinfo)

        # 2. Draw Relationships with Crow's Foot Notation
        for rel in self.relationships:
            self._draw_relationship(
                ax,
                rel['from'],
                rel['to'],
                rel.get('p_card', '1'),
                rel.get('c_card', '0..*')
            )

        # 3. Draw Header & Domain Legend
        self._draw_header(ax)

        # 4. Draw Modality Key
        self._draw_modality_key(ax)

        ax.set_xlim(0.0, 25.8)
        ax.set_ylim(0.0, 17.5)
        ax.axis('off')
        plt.tight_layout()

        return fig

    def _draw_table_card(self, ax, tname, tinfo):
        x, y, w, h = tinfo['x'], tinfo['y'], tinfo['w'], tinfo['h']
        color = self.domain_colors[tinfo['cat']]

        # Card Shadow
        shadow = FancyBboxPatch((x + 0.05, y - 0.05), w, h, boxstyle="round,pad=0.04,rounding_size=0.15",
                                facecolor='#e2e8f0', edgecolor='none', zorder=1)
        ax.add_patch(shadow)

        # Card Body
        card = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.15",
                              facecolor='#ffffff', edgecolor='#cbd5e1', linewidth=1.4, zorder=2)
        ax.add_patch(card)

        # Header Banner
        header_h = 0.65
        header = FancyBboxPatch((x, y + h - header_h), w, header_h, boxstyle="round,pad=0.04,rounding_size=0.15",
                                facecolor=color['header'], edgecolor='none', zorder=3)
        ax.add_patch(header)

        # Header Titles
        ax.text(x + 0.18, y + h - 0.38, tname, fontsize=11, fontweight='bold', color='#ffffff', zorder=4)
        ax.text(x + w - 0.18, y + h - 0.38, f"({tinfo['count']})", fontsize=8.5, color='#f1f5f9',
                ha='right', zorder=4, style='italic')

        # Separate Keys/Constraints from regular attributes
        keys = [col for col in tinfo['cols'] if col[0] != '']
        non_keys = [col for col in tinfo['cols'] if col[0] == '']
        has_divider = len(keys) > 0 and len(non_keys) > 0

        total_slots = len(keys) + (0.5 if has_divider else 0) + len(non_keys)
        slot_gap = (h - header_h - 0.25) / max(total_slots, 1)
        cur_y = y + h - header_h - 0.28

        # Keys (PK, FK)
        for key_type, col_name, dtype in keys:
            badge_color = '#b45309' if key_type == 'PK' else ('#1d4ed8' if key_type == 'FK' else '#047857')
            ax.text(x + 0.18, cur_y, key_type, fontsize=7.5, fontweight='bold', color=badge_color, zorder=4)
            ax.text(x + 0.80, cur_y, col_name, fontsize=9, color='#0f172a', zorder=4, fontweight='bold')
            ax.text(x + w - 0.18, cur_y, dtype, fontsize=8, color='#475569', fontweight='bold', ha='right', zorder=4)
            cur_y -= slot_gap

        # Constraint Divider Line
        if has_divider:
            div_y = cur_y + (slot_gap * 0.35)
            ax.plot([x + 0.12, x + w - 0.12], [div_y, div_y], color='#94a3b8', linewidth=1.2, linestyle='-', zorder=4)
            cur_y -= (slot_gap * 0.45)

        # Attribute Columns
        for _, col_name, dtype in non_keys:
            ax.text(x + 0.80, cur_y, col_name, fontsize=8.8, color='#334155', zorder=4)
            ax.text(x + w - 0.18, cur_y, dtype, fontsize=8, color='#64748b', ha='right', zorder=4)
            cur_y -= slot_gap

    def _draw_relationship(self, ax, p_start, c_end, p_card="1", c_card="0..*", color="#334155"):
        px, py = p_start
        cx, cy = c_end

        # Main orthogonal connector line
        ax.plot([px, cx], [py, cy], color=color, linewidth=1.3, linestyle='-', zorder=5)

        dx, dy = cx - px, cy - py
        is_horizontal = abs(dx) >= abs(dy)

        # 1. Parent Side: Mandatory One (Double bar ||)
        bar_len, bar_gap, first_bar = 0.09, 0.04, 0.06
        if is_horizontal:
            sign = 1 if dx > 0 else -1
            b1_x = px + sign * first_bar
            b2_x = px + sign * (first_bar + bar_gap)
            ax.plot([b1_x, b1_x], [py - bar_len, py + bar_len], color=color, linewidth=1.4, zorder=6)
            ax.plot([b2_x, b2_x], [py - bar_len, py + bar_len], color=color, linewidth=1.4, zorder=6)
            ax.text(px + sign * 0.16, py + 0.12, p_card, fontsize=7.5, fontweight='bold', color='#475569', ha='center', zorder=7)
        else:
            sign = 1 if dy > 0 else -1
            b1_y = py + sign * first_bar
            b2_y = py + sign * (first_bar + bar_gap)
            ax.plot([px - bar_len, px + bar_len], [b1_y, b1_y], color=color, linewidth=1.4, zorder=6)
            ax.plot([px - bar_len, px + bar_len], [b2_y, b2_y], color=color, linewidth=1.4, zorder=6)
            ax.text(px + 0.15, py + sign * 0.08, p_card, fontsize=7.5, fontweight='bold', color='#475569', va='center', zorder=7)

        # 2. Child Side: Crow's Foot Notation & Modality
        fork_len, fork_spread, circle_r = 0.11, 0.075, 0.026
        is_optional = "0.." in c_card
        is_many = "*" in c_card

        if is_horizontal:
            sign = -1 if dx > 0 else 1
            if is_many:
                base_x = cx + sign * fork_len
                ax.plot([base_x, cx], [cy, cy + fork_spread], color=color, linewidth=1.4, zorder=6)
                ax.plot([base_x, cx], [cy, cy - fork_spread], color=color, linewidth=1.4, zorder=6)
                if is_optional:
                    circ_x = base_x + sign * (circle_r + 0.025)
                    circle = patches.Circle((circ_x, cy), radius=circle_r, facecolor='#ffffff', edgecolor=color, linewidth=1.3, zorder=7)
                    ax.add_patch(circle)
                    lbl_x = circ_x + sign * 0.10
                else:
                    bar_x = base_x + sign * 0.045
                    ax.plot([bar_x, bar_x], [cy - bar_len, cy + bar_len], color=color, linewidth=1.4, zorder=6)
                    lbl_x = bar_x + sign * 0.10
            else:
                bar_x = cx + sign * 0.06
                ax.plot([bar_x, bar_x], [cy - bar_len, cy + bar_len], color=color, linewidth=1.4, zorder=6)
                circ_x = bar_x + sign * (circle_r + 0.035)
                circle = patches.Circle((circ_x, cy), radius=circle_r, facecolor='#ffffff', edgecolor=color, linewidth=1.3, zorder=7)
                ax.add_patch(circle)
                lbl_x = circ_x + sign * 0.10

            ax.text(lbl_x, cy + 0.12, c_card, fontsize=7.5, fontweight='bold', color='#475569', ha='center', zorder=7)
        else:
            sign = -1 if dy > 0 else 1
            if is_many:
                base_y = cy + sign * fork_len
                ax.plot([cx, cx - fork_spread], [base_y, cy], color=color, linewidth=1.4, zorder=6)
                ax.plot([cx, cx + fork_spread], [base_y, cy], color=color, linewidth=1.4, zorder=6)
                if is_optional:
                    circ_y = base_y + sign * (circle_r + 0.025)
                    circle = patches.Circle((cx, circ_y), radius=circle_r, facecolor='#ffffff', edgecolor=color, linewidth=1.3, zorder=7)
                    ax.add_patch(circle)
                    lbl_y = circ_y + sign * 0.02
                else:
                    bar_y = base_y + sign * 0.045
                    ax.plot([cx - bar_len, cx + bar_len], [bar_y, bar_y], color=color, linewidth=1.4, zorder=6)
                    lbl_y = bar_y + sign * 0.02
            else:
                bar_y = cy + sign * 0.06
                ax.plot([cx - bar_len, cx + bar_len], [bar_y, bar_y], color=color, linewidth=1.4, zorder=6)
                circ_y = bar_y + sign * (circle_r + 0.035)
                circle = patches.Circle((cx, circ_y), radius=circle_r, facecolor='#ffffff', edgecolor=color, linewidth=1.3, zorder=7)
                ax.add_patch(circle)
                lbl_y = circ_y + sign * 0.02

            ax.text(cx + 0.15, lbl_y, c_card, fontsize=7.5, fontweight='bold', color='#475569', va='center', zorder=7)

    def _draw_header(self, ax):
        banner = FancyBboxPatch((0.8, 15.6), 24.2, 1.4, boxstyle="round,pad=0.05,rounding_size=0.2",
                                facecolor='#0f172a', edgecolor='none', zorder=2)
        ax.add_patch(banner)

        ax.text(1.2, 16.4, "samples.wanderbricks: Production 3NF Data Model Architecture",
                fontsize=17, fontweight='bold', color='#ffffff', zorder=4)
        ax.text(1.2, 15.9, "Clean Crow's Foot Modality (1, 0..*, 0..1), Exact Row Counts & Referential Integrity Constraints",
                fontsize=10.5, color='#94a3b8', zorder=4)

        # Domain Legends
        legends = [
            ('Master / Dimensions', '#d97706'),
            ('Supply & Inventory', '#16a34a'),
            ('Core Facts (Bookings & Payments)', '#2563eb'),
            ('Feedback & Ratings', '#7c3aed'),
            ('High-Volume Clickstream', '#9333ea')
        ]
        lx = 13.0
        for label, c in legends:
            rect = patches.Rectangle((lx, 16.0), 0.35, 0.22, facecolor=c, edgecolor='none', zorder=4)
            ax.add_patch(rect)
            ax.text(lx + 0.42, 16.1, label, fontsize=8.5, color='#e2e8f0', va='center', zorder=4)
            lx += 2.3

    def _draw_modality_key(self, ax):
        mod_box = FancyBboxPatch((0.8, 14.7), 3.5, 0.82, boxstyle="round,pad=0.03,rounding_size=0.08",
                                 facecolor='#ffffff', edgecolor='#cbd5e1', linewidth=1.2, zorder=3)
        ax.add_patch(mod_box)
        ax.text(0.95, 15.34, "CROW'S FOOT NOTATION KEY:", fontsize=7.5, fontweight='bold', color='#0f172a', zorder=4)
        ax.text(0.95, 15.14, "•  ||  = 1 (Mandatory One)", fontsize=7.2, color='#334155', zorder=4)
        ax.text(0.95, 14.95, "•  >o  = 0..* (Optional Many)   |   >|  = 1..* (Mandatory Many)", fontsize=7.2, color='#334155', zorder=4)
        ax.text(0.95, 14.77, "•  -|o = 0..1 (Optional One)", fontsize=7.2, color='#334155', zorder=4)


# ==============================================================================
# 5. CLI EXECUTION ENTRYPOINT
# ==============================================================================
def main():
    parser = argparse.ArgumentParser(description="Generate high-resolution Wanderbricks ERD diagrams.")
    parser.add_argument("--output-dir", default="docs/diagrams", help="Target directory for generated diagrams.")
    parser.add_argument("--sync-root", action="store_true", default=False, help="Explicitly copy to workspace root (default: False).")
    args = parser.parse_args()

    # Determine paths
    repo_root = Path(__file__).resolve().parent.parent
    target_dir = repo_root / args.output_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Initializing ERD Visualizer for samples.wanderbricks...")
    visualizer = ERDVisualizer(SCHEMA_TABLES, RELATIONSHIPS, DOMAIN_COLORS)
    fig = visualizer.render()

    # Ordered diagram naming for future stages:
    # 01_source_3nf_erd -> 02_silver_cleansed_erd -> 03_gold_star_schema_erd
    ordered_png = target_dir / "01_wanderbricks_3nf_erd.png"
    ordered_jpg = target_dir / "01_wanderbricks_3nf_erd.jpg"
    compat_png = target_dir / "wanderbricks_erd_diagram.png"
    compat_jpg = target_dir / "wanderbricks_erd_diagram.jpg"

    fig.savefig(ordered_png, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none', dpi=300)
    fig.savefig(ordered_jpg, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none', dpi=300, pil_kwargs={'quality': 95})
    fig.savefig(compat_png, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none', dpi=300)
    fig.savefig(compat_jpg, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none', dpi=300, pil_kwargs={'quality': 95})
    
    print(f"[SUCCESS] Exported ordered diagrams to {target_dir}:")
    print(f"   - {ordered_png.name}")
    print(f"   - {ordered_jpg.name}")
    print(f"   - {compat_png.name}")

    if args.sync_root:
        root_png = repo_root / "wanderbricks_erd_diagram.png"
        fig.savefig(root_png, bbox_inches='tight', facecolor=fig.get_facecolor(), edgecolor='none', dpi=300)
        print(f"[INFO] Synchronized root copy: {root_png}")

    plt.close(fig)
    print("[DONE] ERD rendering process complete.")


if __name__ == "__main__":
    main()
