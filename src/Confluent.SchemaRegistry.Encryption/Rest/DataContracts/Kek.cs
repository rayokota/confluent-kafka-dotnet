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

using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Confluent.SchemaRegistry.Encryption
{
    [DataContract]
    public class Kek
    {
        /// <summary>
        ///     The name of the KEK.
        /// </summary>
        [DataMember(Name = "name")]
        public new string Name { get; set; }

        /// <summary>
        ///     The KMS type for the KEK.
        /// </summary>
        [DataMember(Name = "kmsType")]
        public new string KmsType { get; set; }

        /// <summary>
        ///     The KMS key ID for the KEK
        /// </summary>
        [DataMember(Name = "kmsKeyId")] 
        public new string KmsKeyId { get; set; }
        
        /// <summary>
        ///     The KMS properties.
        /// </summary>
        [DataMember(Name = "kmsProps")]
        public new IDictionary<string, string> KmsProps { get; set; }

        /// <summary>
        ///     The doc for the KEK.
        /// </summary>
        [DataMember(Name = "doc")] 
        public new string Doc { get; set; }
        
        /// <summary>
        ///     Whether the KEK is shared.
        /// </summary>
        [DataMember(Name = "shared")]
        public new bool Shared { get; set; }
        
        /// <summary>
        ///     Whether the KEK is deleted.
        /// </summary>
        [DataMember(Name = "deleted")]
        public new bool Deleted { get; set; }
    }
}