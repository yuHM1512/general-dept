# Design System Strategy: The Authoritative Canvas

## 1. Overview & Creative North Star
**Creative North Star: "The Ethical Architect"**

This design system rejects the "dashboard-in-a-box" aesthetic in favor of a high-end, editorial experience. For a Social Responsibility project centered on Living Wage surveys, the UI must command respect and project absolute integrity. We achieve this through **The Ethical Architect**—a philosophy that balances the rigid precision of data with the breathing room of a luxury publication.

We move beyond the standard grid by utilizing **intentional asymmetry** and **tonal depth**. By breaking the "template" look with oversized display type and staggered content blocks, we signal that this isn't just a generic tool—it is a sophisticated platform for social change. We prioritize white space not as "empty room," but as a structural element that allows complex data to breathe.

---

## 2. Colors & Surface Philosophy
The palette utilizes deep, authoritative blues (`primary`) and growth-oriented greens (`secondary` and `tertiary`) to create a sense of stable progress.

### The "No-Line" Rule
To achieve a premium, custom feel, **1px solid borders are strictly prohibited for sectioning.** Structural boundaries must be defined solely through background shifts. For example:
- A `surface-container-low` section sitting on a `surface` background.
- A `surface-container-highest` card nested within a `surface-container-low` area.

### Surface Hierarchy & Nesting
Treat the UI as a series of physical layers—like stacked sheets of fine, heavy-weight paper.
*   **Base:** `surface` (#f9f9fa)
*   **Secondary Content:** `surface-container-low` (#f3f3f4)
*   **Primary Cards:** `surface-container-lowest` (#ffffff) for maximum lift and clarity.

### The "Glass & Gradient" Rule
Standard flat colors feel "out-of-the-box." To elevate the experience:
*   **Signature Textures:** Use subtle linear gradients for primary CTAs, transitioning from `primary` (#004275) to `primary_container` (#005a9c). This adds "soul" and a tactile, polished quality.
*   **Glassmorphism:** For floating navigation or modal overlays, use `surface` at 80% opacity with a `24px` backdrop-blur. This keeps the user grounded in the data context while providing a modern, airy feel.

---

## 3. Typography: The Editorial Voice
We utilize a dual-typeface system to balance authority with accessibility.

*   **Display & Headlines (Manrope):** We use Manrope for all `display` and `headline` scales. Its geometric yet warm character provides a "Signature" look that feels more custom than standard sans-serifs. 
    *   *Strategic Use:* Use `display-lg` (3.5rem) with `primary` color for key data points (e.g., the Living Wage figure) to create an undeniable focal point.
*   **Interface & Body (Inter):** Inter is our workhorse. Its high x-height ensures readability in complex survey tables and dense reports.
    *   *Strategic Use:* Use `label-md` in `on_surface_variant` (#414750) for metadata to ensure it feels supportive rather than distracting.

---

## 4. Elevation & Depth
In this system, depth is a function of light and layering, not artificial borders.

*   **The Layering Principle:** Achieve hierarchy by "stacking" the surface-container tiers. Place a `surface-container-lowest` card on a `surface-container-low` section to create a soft, natural lift.
*   **Ambient Shadows:** If a floating element (like a filter popover) requires a shadow, use a "Tinted Ambient" approach:
    *   Blur: `40px` | Opacity: `6%` | Color: Derived from `on_surface` (#1a1c1d). This mimics natural light rather than a harsh digital shadow.
*   **The "Ghost Border" Fallback:** If a border is required for accessibility, use the `outline_variant` (#c1c7d2) at **20% opacity**. Never use a 100% opaque border.
*   **Visual Soul:** Data visualizations should use the `tertiary` (#004940) and `secondary` (#1b6d24) ranges to represent growth and health, avoiding high-vibrancy "neon" colors in favor of these more sophisticated, muted tones.

---

## 5. Components

### Buttons
*   **Primary:** A gradient-filled container (`primary` to `primary_container`) with `on_primary` text. Use `rounded-md` (0.375rem) for a professional, stable feel.
*   **Tertiary (Ghost):** No background or border. Use `primary` text. These should be used for secondary survey actions to keep the visual field clean.

### Input Fields
*   **The "Quiet" Input:** Use `surface_container_high` as the background with no border. Upon focus, transition the background to `surface_container_lowest` and add a 2pt "Ghost Border" using the `primary` color at 40% opacity.

### Cards & Data Lists
*   **Forbid Divider Lines:** Separate list items using vertical white space (8px or 16px from the spacing scale) or subtle alternating background shifts between `surface` and `surface_container_low`.
*   **The Data Hero Card:** Use `surface_container_lowest` with a `xl` (0.75rem) corner radius. This is the primary vessel for survey results and charts.

### Survey Progress Indicators
*   Avoid the "stepper" look. Use a thin, full-width bar at the top of the viewport using a `tertiary_fixed_dim` (#7ad7c6) track and a `tertiary` (#004940) indicator. This feels like an editorial progress bar rather than a form wizard.

---

## 6. Do's and Don'ts

### Do:
*   **Use Asymmetric Padding:** On large screens, allow the left margin of the headline to be wider than the right to create an editorial, "un-templated" feel.
*   **Tone-on-Tone:** Use `primary_fixed` backgrounds with `on_primary_fixed` text for high-importance callouts.
*   **Accessibility First:** Ensure all data visualizations maintain high contrast, specifically when using `secondary` and `tertiary` greens together.

### Don't:
*   **Don't use pure black:** Use `on_surface` (#1a1c1d) for text to keep the "ink" looking premium and soft.
*   **Don't use 1px dividers:** Use a change in surface color or 24px of empty space instead.
*   **Don't "Box" the data:** Avoid wrapping charts in heavy borders. Let the data visualization sit directly on the `surface-container-lowest` card, utilizing the card's edges as the natural boundary.