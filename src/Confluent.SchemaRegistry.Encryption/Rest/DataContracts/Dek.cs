// Copyright 2024 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System.Runtime.Serialization;

namespace Confluent.SchemaRegistry.Encryption
{
    [DataContract]
    public class Dek
    {
        /// <summary>
        ///     The subject the DEK is registered under.
        /// </summary>
        [DataMember(Name = "subject")]
        public new string Subject { get; set; }

        /// <summary>
        ///     The DEK version.
        /// </summary>
        [DataMember(Name = "version")]
        public new int? Version { get; set; }

        /// <summary>
        ///     The DEK algorithm.
        /// </summary>
        [DataMember(Name = "algorithm")] 
        public new DekFormat Algorithm { get; set; }
        
        /// <summary>
        ///     The encrypted key material.
        /// </summary>
        [DataMember(Name = "encryptedKeyMaterial")]
        public new string EncryptedKeyMaterial { get; init; }

        /// <summary>
        ///     Whether the DEK is deleted.
        /// </summary>
        [DataMember(Name = "deleted")]
        public new bool Deleted { get; set; }
    }
}