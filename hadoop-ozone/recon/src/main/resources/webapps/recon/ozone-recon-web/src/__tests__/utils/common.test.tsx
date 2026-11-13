/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, expect, it } from 'vitest';

import { escapeHtml } from '@/utils/common';

describe('escapeHtml', () => {
  it('escapes HTML special characters', () => {
    expect(escapeHtml(`<i>"a"&'b'</i>`)).toEqual('&lt;i&gt;&quot;a&quot;&amp;&#39;b&#39;&lt;/i&gt;');
  });

  it('keeps plain names and handles nullish values', () => {
    expect(escapeHtml('vol1/bucket-2/key_3.txt')).toEqual('vol1/bucket-2/key_3.txt');
    expect(escapeHtml(undefined)).toEqual('');
    expect(escapeHtml(null)).toEqual('');
  });
});
