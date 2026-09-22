package com.github.xingjianluo.metricsalertenginejava.store;

import com.github.xingjianluo.metricsalertenginejava.model.Labels;
import com.github.xingjianluo.metricsalertenginejava.model.Notification;
import com.github.xingjianluo.metricsalertenginejava.model.RuleVersion;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 通知记录：实际发出与被静默抑制的通知都登记，便于审计与去重判断。
 */
public class NotificationLog {

    private final CopyOnWriteArrayList<Notification> notifications = new CopyOnWriteArrayList<>();

    public void append(Notification notification) {
        notifications.add(notification);
    }

    public List<Notification> all() {
        return new ArrayList<>(notifications);
    }

    public List<Notification> forRule(RuleVersion version) {
        List<Notification> out = new ArrayList<>();
        for (Notification n : notifications) {
            if (n.ruleVersion().equals(version)) {
                out.add(n);
            }
        }
        return out;
    }

    /** 最近一次实际发出（非抑制）通知的时间戳；没有则 -1。 */
    public long lastEmittedMillis(RuleVersion version, Labels labels) {
        long last = -1L;
        for (Notification n : notifications) {
            if (!n.suppressed()
                    && n.ruleVersion().ruleId().equals(version.ruleId())
                    && n.labels().equals(labels)
                    && n.timestampMillis() > last) {
                last = n.timestampMillis();
            }
        }
        return last;
    }
}
