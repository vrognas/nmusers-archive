(function () {
  var activeRow = null;
  var activeIndex = -1;

  function getVisibleRows() {
    var tables = document.querySelectorAll('.msg-table');
    for (var i = 0; i < tables.length; i++) {
      if (tables[i].offsetParent !== null) {
        return Array.from(tables[i].querySelectorAll('tbody tr')).filter(function (tr) {
          return tr.offsetParent !== null && !tr.classList.contains('msg-hidden');
        });
      }
    }
    return [];
  }

  function clearActive() {
    if (activeRow) activeRow.classList.remove('row-active');
    activeRow = null;
    activeIndex = -1;
  }

  function setActive(index) {
    var rows = getVisibleRows();
    if (rows.length === 0) return;
    if (activeRow) activeRow.classList.remove('row-active');
    activeIndex = Math.max(0, Math.min(index, rows.length - 1));
    activeRow = rows[activeIndex];
    activeRow.classList.add('row-active');
    activeRow.scrollIntoView({ block: 'nearest' });
  }

  function openActiveRow() {
    if (!activeRow) return;
    var link = activeRow.querySelector('.subject-col a') || activeRow.querySelector('a');
    if (link) window.location.href = link.href;
  }

  function showToast(msg) {
    var el = document.getElementById('kbd-toast');
    if (!el) {
      el = document.createElement('div');
      el.id = 'kbd-toast';
      el.className = 'kbd-toast';
      document.body.appendChild(el);
    }
    el.textContent = msg;
    el.classList.add('visible');
    clearTimeout(el._timer);
    el._timer = setTimeout(function () { el.classList.remove('visible'); }, 1500);
  }

  var helpEl = document.getElementById('kbd-help');
  var previousFocus = null;

  function openHelp() {
    previousFocus = document.activeElement;
    helpEl.classList.add('visible');
    helpEl.querySelector('.kbd-help-inner').focus();
  }

  function closeHelp() {
    helpEl.classList.remove('visible');
    if (previousFocus) previousFocus.focus();
  }

  helpEl.addEventListener('click', function (e) {
    if (e.target === helpEl) closeHelp();
  });

  // Focus trap: keep Tab cycling inside the dialog
  helpEl.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') { e.preventDefault(); closeHelp(); return; }
    if (e.key !== 'Tab') return;
    var focusable = helpEl.querySelectorAll('a, button, [tabindex]:not([tabindex="-1"])');
    if (focusable.length === 0) { e.preventDefault(); return; }
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  });

  document.addEventListener('keydown', function (e) {
    // When help dialog is open, only handle Escape (handled by dialog's own listener)
    if (helpEl.classList.contains('visible')) return;

    // Don't intercept when modifier keys are held (Cmd+F, Ctrl+F, etc.)
    if (e.metaKey || e.ctrlKey || e.altKey) return;

    var tag = document.activeElement.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
      if (e.key === 'Escape') document.activeElement.blur();
      return;
    }

    switch (e.key) {
      case 'j':
        e.preventDefault();
        setActive(activeIndex + 1);
        break;
      case 'k':
        e.preventDefault();
        setActive(activeIndex - 1);
        break;
      case 'o':
      case 'Enter':
        if (activeRow) { e.preventDefault(); openActiveRow(); }
        break;
      case 'f':
        e.preventDefault();
        var search = document.getElementById('search-input')
          || document.getElementById('search-input-sticky')
          || document.getElementById('author-search')
          || document.getElementById('thread-search');
        if (search) search.focus();
        break;
      case 's':
        e.preventDefault();
        var url = window.location.href;
        if (activeRow) {
          var a = activeRow.querySelector('.subject-col a') || activeRow.querySelector('a');
          if (a) url = a.href;
        }
        navigator.clipboard.writeText(url).then(function () {
          showToast('Link copied');
        });
        break;
      case 't':
        e.preventDefault();
        var sidebar = document.querySelector('.filter-sidebar');
        var sortCtrl = document.querySelector('.sort-controls');
        var target = sidebar || sortCtrl;
        if (target) {
          target.scrollIntoView({ block: 'nearest' });
          var btn = target.querySelector('button');
          if (btn) btn.focus();
        }
        break;
      case 'c':
        e.preventDefault();
        var body = document.querySelector('.msg-body');
        if (body) {
          navigator.clipboard.writeText(body.textContent).then(function () {
            showToast('Message copied');
          });
        }
        break;
      case 'r':
        e.preventDefault();
        var threads = document.querySelector('.thread-list');
        if (threads) threads.scrollIntoView({ block: 'start' });
        break;
      case '?':
        e.preventDefault();
        openHelp();
        break;
      case 'Escape':
        clearActive();
        break;
    }
  });
})();
