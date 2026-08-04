// ui/appshell/blocks/tabs.js — RFC 028 Part 4 (V2) tabs block renderer.
//
// Security (spec §6.4): tab labels are UNTRUSTED text — rendered via
// textContent, never innerHTML. Nested blocks are rendered through the shared
// renderBlock dispatch (shell.js), so every nested block is sanitized by the
// same per-type renderer the top level uses — there is no second render path.
//
// Interactivity (spec §6.3: "a `tabs` block (all blocks delivered, shell
// switches client-side)"): all tabs' blocks arrive in the doc; switching is
// purely client-side (no re-render, no query). Only the active tab's blocks are
// mounted, and the panel is rebuilt on switch from the already-delivered data
// (no network) — so a hidden tab's chart does not mount until its tab is shown.
//
// Tab shape (W1 tabsBlock schema): tabs[]{label, blocks[]}.

import { renderBlock } from "../shell.js";

// renderTabs builds a client-side tab strip + panel for the block.
export function renderTabs(block) {
  const el = document.createElement("div");
  el.className = "dtp-block dtp-block-tabs";

  const tabs = block && Array.isArray(block.tabs) ? block.tabs : [];

  const bar = document.createElement("div");
  bar.className = "dtp-tab-bar";
  bar.setAttribute("role", "tablist");

  const panel = document.createElement("div");
  panel.className = "dtp-tab-panel";

  const buttons = [];

  function show(index) {
    panel.textContent = "";
    const tab = tabs[index];
    const blocks = tab && Array.isArray(tab.blocks) ? tab.blocks : [];
    for (const nested of blocks) panel.appendChild(renderBlock(nested));
    for (let i = 0; i < buttons.length; i++) {
      const selected = i === index;
      buttons[i].classList.toggle("dtp-tab-active", selected);
      buttons[i].setAttribute("aria-selected", selected ? "true" : "false");
    }
  }

  tabs.forEach((tab, i) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "dtp-tab";
    button.setAttribute("role", "tab");
    button.textContent = tab && typeof tab.label === "string" ? tab.label : "Tab " + (i + 1);
    button.addEventListener("click", () => show(i));
    buttons.push(button);
    bar.appendChild(button);
  });

  el.appendChild(bar);
  el.appendChild(panel);

  if (tabs.length > 0) show(0);

  return el;
}
