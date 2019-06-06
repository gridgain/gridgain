/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 *
 */
public final class MpscQueue<E> extends MpscQueue_P1 {
    /** Block marker. */
    private static final Node BLOCKED = new Node(null);
    
    /** Head. */
    protected final AtomicReference<Node> headRef = new AtomicReference<>();
    
    /** Consumer thread. */
    private volatile Thread consumer;

    /**
     * Poll element.
     *
     * @return Element.
     */
    public E poll() {
        if (tailSize > 0)
            return (E)tail[--tailSize];

        Node head = headRef.getAndSet(null);

        if (head != null) {
            int size = head.size;

            if (tail.length < size)
                tail = new Object[Integer.highestOneBit(size) << 1];

            for (int i = 0; i < size ; i++) {
                tail[i] = head.val;
                head = head.next;
            }

            return (E)tail[tailSize = size - 1];
        }

        return null;
    }

    /**
     * Take element.
     *
     * @return Element.
     */
    public E take() throws InterruptedException {
        if (tailSize > 0)
            return (E)tail[--tailSize];

        AtomicReference<Node> headRef = this.headRef;

        for (; ; ) {
            Node head = headRef.getAndSet(null);

            if (head != null) {
                int size = head.size;

                if (tail.length < size)
                    tail = new Object[Integer.highestOneBit(size) << 1];

                for (int i = 0; i < size ; i++) {
                    tail[i] = head.val;
                    head = head.next;
                }

                return (E)tail[tailSize = size - 1];
            }

            if (headRef.compareAndSet(null, BLOCKED)) {
                do {
                    if (consumer == null)
                        consumer = Thread.currentThread();
                    else
                        LockSupport.park();

                    if (Thread.interrupted())
                        throw new InterruptedException();
                }
                while (headRef.get() == BLOCKED);
            }
        }
    }

    /**
     * Offer element.
     *
     * @param e Element.
     */
    public void offer(final E e) {
        if (e == null)
            throw new IllegalArgumentException("Null are not allowed.");

        final Node newItem = new Node(e);

        for (; ; ) {
            Node head = headRef.get();

            if (head == null || head == BLOCKED) {
                newItem.next = null;
                newItem.size = 1;
            }
            else {
                newItem.next = head;
                newItem.size = head.size + 1;
            }

            if (headRef.compareAndSet(head, newItem)) {
                if (head == BLOCKED)
                    LockSupport.unpark(consumer);

                break;
            }
        }
    }

    /**
     * @return Queue size.
     */
    public int size() {
        int size = tailSize;

        Node head = headRef.get();

        if (head != null)
            size += head.size;

        return size;
    }

    @Override public String toString() {
        return "MpscQueue[size=" + size() + ']';
    }
}

abstract class MpscQueue_P1 extends MpscQueue_P2 {
    boolean p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    boolean p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    boolean p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    boolean p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
    boolean p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    boolean p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    boolean p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    boolean p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
    boolean p128, p129, p130, p131, p132, p133, p134, p135, p136, p137, p138, p139, p140, p141, p142, p143;
    boolean p144, p145, p146, p147, p148, p149, p150, p151, p152, p153, p154, p155, p156, p157, p158, p159;
    boolean p160, p161, p162, p163, p164, p165, p166, p167, p168, p169, p170, p171, p172, p173, p174, p175;
    boolean p176, p177, p178, p179, p180, p181, p182, p183, p184, p185, p186, p187, p188, p189, p190, p191;
    boolean p192, p193, p194, p195, p196, p197, p198, p199, p200, p201, p202, p203, p204, p205, p206, p207;
    boolean p208, p209, p210, p211, p212, p213, p214, p215, p216, p217, p218, p219, p220, p221, p222, p223;
    boolean p224, p225, p226, p227, p228, p229, p230, p231, p232, p233, p234, p235, p236, p237, p238, p239;
    boolean p240, p241, p242, p243, p244, p245, p246, p247, p248, p249, p250, p251, p252, p253, p254, p255;
}

abstract class MpscQueue_P2 extends MpscQueue_P3 {
    /** Tail. */
    protected Object[] tail = new Object[256];
    /** Tail size. */
    protected int tailSize;
}

abstract class MpscQueue_P3 {
    boolean p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    boolean p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    boolean p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    boolean p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
    boolean p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    boolean p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    boolean p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    boolean p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
    boolean p128, p129, p130, p131, p132, p133, p134, p135, p136, p137, p138, p139, p140, p141, p142, p143;
    boolean p144, p145, p146, p147, p148, p149, p150, p151, p152, p153, p154, p155, p156, p157, p158, p159;
    boolean p160, p161, p162, p163, p164, p165, p166, p167, p168, p169, p170, p171, p172, p173, p174, p175;
    boolean p176, p177, p178, p179, p180, p181, p182, p183, p184, p185, p186, p187, p188, p189, p190, p191;
    boolean p192, p193, p194, p195, p196, p197, p198, p199, p200, p201, p202, p203, p204, p205, p206, p207;
    boolean p208, p209, p210, p211, p212, p213, p214, p215, p216, p217, p218, p219, p220, p221, p222, p223;
    boolean p224, p225, p226, p227, p228, p229, p230, p231, p232, p233, p234, p235, p236, p237, p238, p239;
    boolean p240, p241, p242, p243, p244, p245, p246, p247, p248, p249, p250, p251, p252, p253, p254, p255;
}

final class Node {
    /** Value. */
    final Object val;

    /** Next node. */
    Node next;

    /** */
    int size;

    /**
     * Constructor.
     *
     * @param val Value.
     */
    Node(Object val) {
        this.val = val;
    }
}

