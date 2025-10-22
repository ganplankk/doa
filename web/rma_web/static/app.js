document.addEventListener("DOMContentLoaded", () => {
  // Flatpickr: 네이티브 select 대신 static 헤더로 (브라우저/OS 영향 제거)
  const fpCommon = {
    dateFormat: "Y-m-d",
    locale: "ko",
    allowInput: true,
    monthSelectorType: "static",
    disableMobile: true
  };
  flatpickr("#install_date", fpCommon);
  flatpickr("#failure_date", fpCommon);

  const form = document.getElementById("rmaForm");
  const submitBtn = document.getElementById("submitBtn");
  const toast = document.getElementById("toast");
  const requiredNames = ["name","company","email","model","serial_number","initial_install_date"];

  const showToast = (ok, msg) => {
    toast.className = ok ? "toast-ok" : "toast-err";
    toast.textContent = msg;
  };

  const validate = () => {
    let ok = true;
    // 초기화
    form.querySelectorAll(".field").forEach(f => f.classList.remove("invalid"));

    for (const name of requiredNames) {
      const el = form.querySelector(`[name="${name}"]`);
      if (!el) continue;
      const wrap = el.closest(".field");
      const value = (el.value || "").trim();

      let bad = false;
      if (!value) bad = true;
      if (!bad && name === "email" && !/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(value)) {
        bad = true;
        const msg = wrap.querySelector(".error");
        if (msg) msg.textContent = "올바른 이메일을 입력하세요.";
      }
      if (bad) { wrap.classList.add("invalid"); ok = false; }
    }
    return ok;
  };

  // 서버 POST with timeout
  const postJSON = (url, body, timeoutMs = 10000) => {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    return fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: ctrl.signal
    }).finally(() => clearTimeout(t));
  };

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    showToast(true, ""); // 초기화

    if (!validate()) {
      showToast(false, "입력값을 확인해주세요.");
      return;
    }

    const payload = Object.fromEntries(new FormData(form).entries());
    submitBtn.disabled = true;

    try {
      const r = await postJSON("/api/rma", payload);
      const data = await r.json();
      if (r.ok && data.ok) {
        showToast(true, `등록 완료 (TICKET ID: ${data.id})`);
        form.reset();
      } else if (data && data.error === "validation_failed" && data.fieldErrors) {
        // 서버 검증 오류를 필드에 반영
        Object.keys(data.fieldErrors).forEach((field) => {
          const el = form.querySelector(`[name="${field}"]`);
          if (!el) return;
          const wrap = el.closest(".field");
          const msg = wrap.querySelector(".error");
          if (msg) msg.textContent = data.fieldErrors[field];
          wrap.classList.add("invalid");
        });
        showToast(false, "입력값을 확인해주세요.");
      } else {
        showToast(false, "처리 중 오류가 발생했습니다.");
      }
    } catch {
      showToast(false, "네트워크 오류 또는 타임아웃입니다.");
    } finally {
      submitBtn.disabled = false;
    }
  });
});

