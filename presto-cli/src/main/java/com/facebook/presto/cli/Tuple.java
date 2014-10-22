/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.cli;

public class Tuple {
	public static class Pair<K, V> {
		private K Key;
		private V Value;

		public Pair(K key, V value) {
			Key = key;
			Value = value;
		}

		public K getKey() {
			return Key;
		}

		public V getValue() {
			return Value;
		}

		public void SetKey(K key) {
			Key = key;
		}

		public void SetValue(V value) {
			Value = value;
		}
	}

	public static class Triple<K, M, V> {
		private K Key;
		private M Middle;
		private V Value;

		public Triple(K key, M middle, V value) {
			Key = key;
			Middle = middle;
			Value = value;
		}

		public K getKey() {
			return Key;
		}

		public M getMiddle() {
			return Middle;
		}

		public V getValue() {
			return Value;
		}

		public void SetKey(K key) {
			Key = key;
		}

		public void SetMiddle(M mid) {
			Middle = mid;
		}

		public void SetValue(V value) {
			Value = value;
		}
	}

	public static class Quadruple<F1, F2, F3, F4> {
		private F1 F1;
		private F2 F2;
		private F3 F3;
		private F4 F4;

		public Quadruple(F1 f1, F2 f2, F3 f3, F4 f4) {
			F1 = f1;
			F2 = f2;
			F3 = f3;
			F4 = f4;
		}

		public F1 getF1() {
			return F1;
		}

		public F2 getF2() {
			return F2;
		}

		public F3 getF3() {
			return F3;
		}

		public F4 getF4() {
			return F4;
		}

		public void setF1(F1 f1) {
			F1 = f1;
		}

		public void setF2(F2 f2) {
			F2 = f2;
		}

		public void setF3(F3 f3) {
			F3 = f3;
		}

		public void setF4(F4 f4) {
			F4 = f4;
		}
	}
}
