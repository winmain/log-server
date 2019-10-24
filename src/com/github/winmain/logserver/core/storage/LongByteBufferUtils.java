package com.github.winmain.logserver.core.storage;

public class LongByteBufferUtils {
    /**
     * The maximum number of runs in merge sort.
     */
    private static final int MAX_RUN_COUNT = 67;

    /**
     * The maximum length of run in merge sort.
     */
    private static final int MAX_RUN_LENGTH = 33;

    /**
     * If the length of an array to be sorted is less than this
     * constant, Quicksort is used in preference to merge sort.
     */
    private static final int QUICKSORT_THRESHOLD = 286;

    /**
     * If the length of an array to be sorted is less than this
     * constant, insertion sort is used in preference to Quicksort.
     */
    private static final int INSERTION_SORT_THRESHOLD = 47;

    /**
     * Sorts the specified range of the array using the given
     * workspace array slice if possible for merging
     *
     * @param a the array to be sorted
     * @param left the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     * @param work a workspace array (slice)
     * @param workBase origin of usable space in work array
     * @param workLen usable size of work array
     */
    static void sort(LongByteBuffer a, int left, int right,
                     LongByteBuffer work, int workBase, int workLen) {
        // Use Quicksort on small arrays
        if (right - left < QUICKSORT_THRESHOLD) {
            sort(a, left, right, true);
            return;
        }

        /*
         * Index run[i] is the start of i-th run
         * (ascending or descending sequence).
         */
        int[] run = new int[MAX_RUN_COUNT + 1];
        int count = 0; run[0] = left;

        // Check if the array is nearly sorted
        for (int k = left; k < right; run[count] = k) {
            if (a.apply(k) < a.apply(k + 1)) { // ascending
                while (++k <= right && a.apply(k - 1) <= a.apply(k));
            } else if (a.apply(k) > a.apply(k + 1)) { // descending
                while (++k <= right && a.apply(k - 1) >= a.apply(k));
                for (int lo = run[count] - 1, hi = k; ++lo < --hi; ) {
                    long t = a.apply(lo); a.update(lo, a.apply(hi)); a.update(hi, t);
                }
            } else { // equal
                for (int m = MAX_RUN_LENGTH; ++k <= right && a.apply(k - 1) == a.apply(k); ) {
                    if (--m == 0) {
                        sort(a, left, right, true);
                        return;
                    }
                }
            }

            /*
             * The array is not highly structured,
             * use Quicksort instead of merge sort.
             */
            if (++count == MAX_RUN_COUNT) {
                sort(a, left, right, true);
                return;
            }
        }

        // Check special cases
        // Implementation note: variable "right" is increased by 1.
        if (run[count] == right++) { // The last run contains one element
            run[++count] = right;
        } else if (count == 1) { // The array is already sorted
            return;
        }

        // Determine alternation base for merge
        byte odd = 0;
        for (int n = 1; (n <<= 1) < count; odd ^= 1);

        // Use or create temporary array b for merging
        LongByteBuffer b;                 // temp array; alternates with a
        int ao, bo;              // array offsets from 'left'
        int blen = right - left; // space needed for b
        if (work == null || workLen < blen || workBase + blen > work.length()) {
            work = new LongByteBuffer(blen);
            workBase = 0;
        }
        if (odd == 0) {
            a.copyTo(left, work, workBase, blen);
            b = a;
            bo = 0;
            a = work;
            ao = workBase - left;
        } else {
            b = work;
            ao = 0;
            bo = workBase - left;
        }

        // Merging
        for (int last; count > 1; count = last) {
            for (int k = (last = 0) + 2; k <= count; k += 2) {
                int hi = run[k], mi = run[k - 1];
                for (int i = run[k - 2], p = i, q = mi; i < hi; ++i) {
                    if (q >= hi || p < mi && a.apply(p + ao) <= a.apply(q + ao)) {
                        b.update(i + bo, a.apply(p++ + ao));
                    } else {
                        b.update(i + bo, a.apply(q++ + ao));
                    }
                }
                run[++last] = hi;
            }
            if ((count & 1) != 0) {
                for (int i = right, lo = run[count - 1]; --i >= lo;
                     b.update(i + bo, a.apply(i + ao))
                     );
                run[++last] = right;
            }
            LongByteBuffer t = a; a = b; b = t;
            int o = ao; ao = bo; bo = o;
        }
    }

    /**
     * Sorts the specified range of the array by Dual-Pivot Quicksort.
     *
     * @param a the array to be sorted
     * @param left the index of the first element, inclusive, to be sorted
     * @param right the index of the last element, inclusive, to be sorted
     * @param leftmost indicates if this part is the leftmost in the range
     */
    private static void sort(LongByteBuffer a, int left, int right, boolean leftmost) {
        int length = right - left + 1;

        // Use insertion sort on tiny arrays
        if (length < INSERTION_SORT_THRESHOLD) {
            if (leftmost) {
                /*
                 * Traditional (without sentinel) insertion sort,
                 * optimized for server VM, is used in case of
                 * the leftmost part.
                 */
                for (int i = left, j = i; i < right; j = ++i) {
                    long ai = a.apply(i + 1);
                    while (ai < a.apply(j)) {
                        a.update(j + 1, a.apply(j));
                        if (j-- == left) {
                            break;
                        }
                    }
                    a.update(j + 1, ai);
                }
            } else {
                /*
                 * Skip the longest ascending sequence.
                 */
                do {
                    if (left >= right) {
                        return;
                    }
                } while (a.apply(++left) >= a.apply(left - 1));

                /*
                 * Every element from adjoining part plays the role
                 * of sentinel, therefore this allows us to avoid the
                 * left range check on each iteration. Moreover, we use
                 * the more optimized algorithm, so called pair insertion
                 * sort, which is faster (in the context of Quicksort)
                 * than traditional implementation of insertion sort.
                 */
                for (int k = left; ++left <= right; k = ++left) {
                    long a1 = a.apply(k), a2 = a.apply(left);

                    if (a1 < a2) {
                        a2 = a1; a1 = a.apply(left);
                    }
                    while (a1 < a.apply(--k)) {
                        a.update(k + 2, a.apply(k));
                    }
                    a.update(++k + 1, a1);

                    while (a2 < a.apply(--k)) {
                        a.update(k + 1, a.apply(k));
                    }
                    a.update(k + 1, a2);
                }
                long last = a.apply(right);

                while (last < a.apply(--right)) {
                    a.update(right + 1, a.apply(right));
                }
                a.update(right + 1, last);
            }
            return;
        }

        // Inexpensive approximation of length / 7
        int seventh = (length >> 3) + (length >> 6) + 1;

        /*
         * Sort five evenly spaced elements around (and including) the
         * center element in the range. These elements will be used for
         * pivot selection as described below. The choice for spacing
         * these elements was empirically determined to work well on
         * a wide variety of inputs.
         */
        int e3 = (left + right) >>> 1; // The midpoint
        int e2 = e3 - seventh;
        int e1 = e2 - seventh;
        int e4 = e3 + seventh;
        int e5 = e4 + seventh;

        // Sort these elements using insertion sort
        if (a.apply(e2) < a.apply(e1)) { long t = a.apply(e2); a.update(e2, a.apply(e1)); a.update(e1, t); }

        if (a.apply(e3) < a.apply(e2)) { long t = a.apply(e3); a.update(e3, a.apply(e2)); a.update(e2, t);
            if (t < a.apply(e1)) { a.update(e2, a.apply(e1)); a.update(e1, t); }
        }
        if (a.apply(e4) < a.apply(e3)) { long t = a.apply(e4); a.update(e4, a.apply(e3)); a.update(e3, t);
            if (t < a.apply(e2)) { a.update(e3, a.apply(e2)); a.update(e2, t);
                if (t < a.apply(e1)) { a.update(e2, a.apply(e1)); a.update(e1, t); }
            }
        }
        if (a.apply(e5) < a.apply(e4)) { long t = a.apply(e5); a.update(e5, a.apply(e4)); a.update(e4, t);
            if (t < a.apply(e3)) { a.update(e4, a.apply(e3)); a.update(e3, t);
                if (t < a.apply(e2)) { a.update(e3, a.apply(e2)); a.update(e2, t);
                    if (t < a.apply(e1)) { a.update(e2, a.apply(e1)); a.update(e1, t); }
                }
            }
        }

        // Pointers
        int less  = left;  // The index of the first element of center part
        int great = right; // The index before the first element of right part

        if (a.apply(e1) != a.apply(e2) && a.apply(e2) != a.apply(e3) && a.apply(e3) != a.apply(e4) && a.apply(e4) != a.apply(e5)) {
            /*
             * Use the second and fourth of the five sorted elements as pivots.
             * These values are inexpensive approximations of the first and
             * second terciles of the array. Note that pivot1 <= pivot2.
             */
            long pivot1 = a.apply(e2);
            long pivot2 = a.apply(e4);

            /*
             * The first and the last elements to be sorted are moved to the
             * locations formerly occupied by the pivots. When partitioning
             * is complete, the pivots are swapped back into their final
             * positions, and excluded from subsequent sorting.
             */
            a.update(e2, a.apply(left));
            a.update(e4, a.apply(right));

            /*
             * Skip elements, which are less or greater than pivot values.
             */
            while (a.apply(++less) < pivot1);
            while (a.apply(--great) > pivot2);

            /*
             * Partitioning:
             *
             *   left part           center part                   right part
             * +--------------------------------------------------------------+
             * |  < pivot1  |  pivot1 <= && <= pivot2  |    ?    |  > pivot2  |
             * +--------------------------------------------------------------+
             *               ^                          ^       ^
             *               |                          |       |
             *              less                        k     great
             *
             * Invariants:
             *
             *              all in (left, less)   < pivot1
             *    pivot1 <= all in [less, k)     <= pivot2
             *              all in (great, right) > pivot2
             *
             * Pointer k is the first index of ?-part.
             */
                 outer:
            for (int k = less - 1; ++k <= great; ) {
                long ak = a.apply(k);
                if (ak < pivot1) { // Move a.apply(k) to left part
                    a.update(k, a.apply(less));
                    /*
                     * Here and below we use "a.apply(i, b); i++;" instead
                     * of "a.apply(i++, b);" due to performance issue.
                     */
                    a.update(less, ak);
                    ++less;
                } else if (ak > pivot2) { // Move a.apply(k) to right part
                    while (a.apply(great) > pivot2) {
                        if (great-- == k) {
                            break outer;
                        }
                    }
                    if (a.apply(great) < pivot1) { // a.apply(great) <= pivot2
                        a.update(k, a.apply(less));
                        a.update(less, a.apply(great));
                        ++less;
                    } else { // pivot1 <= a.apply(great) <= pivot2
                        a.update(k, a.apply(great));
                    }
                    /*
                     * Here and below we use "a.apply(i, b); i--;" instead
                     * of "a.apply(i--, b);" due to performance issue.
                     */
                    a.update(great, ak);
                    --great;
                }
            }

            // Swap pivots into their final positions
            a.update(left, a.apply(less - 1)); a.update(less - 1, pivot1);
            a.update(right, a.apply(great + 1)); a.update(great + 1, pivot2);

            // Sort left and right parts recursively, excluding known pivots
            sort(a, left, less - 2, leftmost);
            sort(a, great + 2, right, false);

            /*
             * If center part is too large (comprises > 4/7 of the array),
             * swap internal pivot values to ends.
             */
            if (less < e1 && e5 < great) {
                /*
                 * Skip elements, which are equal to pivot values.
                 */
                while (a.apply(less) == pivot1) {
                    ++less;
                }

                while (a.apply(great) == pivot2) {
                    --great;
                }

                /*
                 * Partitioning:
                 *
                 *   left part         center part                  right part
                 * +----------------------------------------------------------+
                 * | == pivot1 |  pivot1 < && < pivot2  |    ?    | == pivot2 |
                 * +----------------------------------------------------------+
                 *              ^                        ^       ^
                 *              |                        |       |
                 *             less                      k     great
                 *
                 * Invariants:
                 *
                 *              all in (*,  less) == pivot1
                 *     pivot1 < all in [less,  k)  < pivot2
                 *              all in (great, *) == pivot2
                 *
                 * Pointer k is the first index of ?-part.
                 */
                     outer:
                for (int k = less - 1; ++k <= great; ) {
                    long ak = a.apply(k);
                    if (ak == pivot1) { // Move a.apply(k) to left part
                        a.update(k, a.apply(less));
                        a.update(less, ak);
                        ++less;
                    } else if (ak == pivot2) { // Move a.apply(k) to right part
                        while (a.apply(great) == pivot2) {
                            if (great-- == k) {
                                break outer;
                            }
                        }
                        if (a.apply(great) == pivot1) { // a.apply(great) < pivot2
                            a.update(k, a.apply(less));
                            /*
                             * Even though a.apply(great) equals to pivot1, the
                             * assignment a.apply(less, pivot1 may be incorrect),
                             * if a.apply(great) and pivot1 are floating-point zeros
                             * of different signs. Therefore in float and
                             * double sorting methods we have to use more
                             * accurate assignment a.apply(less, a.apply(great).
                             */
                            a.update(less, pivot1);
                            ++less;
                        } else { // pivot1 < a.apply(great) < pivot2
                            a.update(k, a.apply(great));
                        }
                        a.update(great, ak);
                        --great;
                    }
                }
            }

            // Sort center part recursively
            sort(a, less, great, false);

        } else { // Partitioning with one pivot
            /*
             * Use the third of the five sorted elements as pivot.
             * This value is inexpensive approximation of the median.
             */
            long pivot = a.apply(e3);

            /*
             * Partitioning degenerates to the traditional 3-way
             * (or "Dutch National Flag") schema:
             *
             *   left part    center part              right part
             * +-------------------------------------------------+
             * |  < pivot  |   == pivot   |     ?    |  > pivot  |
             * +-------------------------------------------------+
             *              ^              ^        ^
             *              |              |        |
             *             less            k      great
             *
             * Invariants:
             *
             *   all in (left, less)   < pivot
             *   all in [less, k)     == pivot
             *   all in (great, right) > pivot
             *
             * Pointer k is the first index of ?-part.
             */
            for (int k = less; k <= great; ++k) {
                if (a.apply(k) == pivot) {
                    continue;
                }
                long ak = a.apply(k);
                if (ak < pivot) { // Move a.apply(k) to left part
                    a.update(k, a.apply(less));
                    a.update(less, ak);
                    ++less;
                } else { // a.apply(k) > pivot - Move a.apply(k) to right part
                    while (a.apply(great) > pivot) {
                        --great;
                    }
                    if (a.apply(great) < pivot) { // a.apply(great) <= pivot
                        a.update(k, a.apply(less));
                        a.update(less, a.apply(great));
                        ++less;
                    } else { // a.apply(great) == pivot
                        /*
                         * Even though a.apply(great) equals to pivot, the
                         * assignment a.apply(k, pivot may be incorrect),
                         * if a.apply(great) and pivot are floating-point
                         * zeros of different signs. Therefore in float
                         * and double sorting methods we have to use
                         * more accurate assignment a.apply(k, a.apply(great).
                         */
                        a.update(k, pivot);
                    }
                    a.update(great, ak);
                    --great;
                }
            }

            /*
             * Sort left and right parts recursively.
             * All elements from center part are equal
             * and, therefore, already sorted.
             */
            sort(a, left, less - 1, leftmost);
            sort(a, great + 1, right, false);
        }
    }
}
