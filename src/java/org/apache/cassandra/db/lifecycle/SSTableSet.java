/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.lifecycle;

public enum SSTableSet
{
    // returns the "canonical" version of any current sstable, i.e. if an sstable is being replaced and is only partially
    // visible to reads, this sstable will be returned as its original entirety, and its replacement will not be returned
    // (even if it completely replaces it)
    CANONICAL,
    // returns the live versions of all sstables, i.e. including partially written sstables
    LIVE,
    // returns the non-compacting sstables, i.e. the difference between live and compacting ones
    NONCOMPACTING
}
