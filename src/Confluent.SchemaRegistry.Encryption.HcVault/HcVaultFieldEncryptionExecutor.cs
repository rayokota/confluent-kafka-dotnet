namespace Confluent.SchemaRegistry.Encryption.HcVault
{
    public class HcVaultFieldEncryptionExecutor : FieldEncryptionExecutor
    {
        public HcVaultFieldEncryptionExecutor(string kmsKeyId, string tokenId)
        {
            kekId = HcVaultKmsClient.Prefix + kmsKeyId;
            KmsClients.Add(kekId, new HcVaultKmsClient(kekId, tokenId));
        }
    }
}