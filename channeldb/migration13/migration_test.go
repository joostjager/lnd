package migration13

import (
	"testing"

	"github.com/coreos/bbolt"
	"github.com/lightningnetwork/lnd/channeldb/migtest"
)

var (
	// pre is the data in the payments root bucket in database version 12 format.
	pre = map[string]interface{}{
		// A failed payment.
		"0x02acee76ebd53d00824410cf6adecad4f50334dac702bd5a2d3ba01b91709f0e": map[string]interface{}{
			"payment-attempt-info":  "0x00000000000000012997a72e129fc9d638ef2fa4e233567d808d4f18a4f087637582427962eb3bf800005ce600000000004c4b402102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000004c4b400000000000",
			"payment-creation-info": "0x02acee76ebd53d00824410cf6adecad4f50334dac702bd5a2d3ba01b91709f0e00000000004c4b40000000005e4fb7ab00000000",
			"payment-fail-info":     "0x03",
			"payment-sequence-key":  "0x0000000000000001",
		},

		// A settled payment.
		"0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840c": map[string]interface{}{
			"payment-attempt-info":  "0x00000000000003e88de663f9bb4b8d1ebdb496d22dc1cb657a346215607308549f41b01e2adf2ce900005ce600000000005b8d802102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000005b8d8000000000010000000000000008233d281e2cbe01f0b82dd6750967c9233426b98ae6549c696365f57f86f942a3795b8d80",
			"payment-creation-info": "0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840c00000000005b8d80000000005e4fb97f000000fc6c6e62637274363075317030796c7774367070357674346e377a6a676c39327766397773633939767630307366666e7561376a656d74376d6535373471336b3337346a387373787164717163717a70677370353835357075743937713863747374776b7735796b306a667278736e746e7a6878326a77786a636d3937346c636437327a3564757339717939717371653872336b3578733379367868667366366d6a6e706d717172306661797a677a63336a6b663571787a6c376866787a6666763578667a7679647564327275767974706571787072376868796830726a747574373033333274737774686661616e303773766b6667716b7174667275",
			"payment-sequence-key":  "0x0000000000000002",
			"payment-settle-info":   "0x479593b7d3cbb45beb22d448451a2f3619b2095adfb38f4d92e9886e96534368",
		},

		// An in-flight payment.
		"0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840d": map[string]interface{}{
			"payment-attempt-info":  "0x00000000000003e953ce0a4c1507cc5ea00ec88b76bd43a3978ac13605497030b821af6ce9c110f300005ce600000000006acfc02102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000006acfc000000000010000000000000008233044f235354472318b381fad3e21eb5a58f5099918868b0610e7b7bcb7a4adc96acfc0",
			"payment-creation-info": "0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840d00000000006acfc0000000005e4fb98d000000fc6c6e62637274373075317030796c7776327070357674346e377a6a676c39327766397773633939767630307366666e7561376a656d74376d6535373471336b3337346a387373787364717163717a706773703578707a307964663467336572727a656372376b6e7567307474667630327a7665727a72676b70737375376d6d6564617934687973397179397173717774656479336e666c323534787a36787a75763974746767757a647473356e617a7461616a6735667772686438396b336d70753971726d7a6c3779637a306e30666e6e763077753032726632706e64636c393761646c667636376a7a6e7063677477356434366771323571326e32",
			"payment-sequence-key":  "0x0000000000000003",
		},
	}

	// post is the expected data after migration.
	post = map[string]interface{}{
		"0x02acee76ebd53d00824410cf6adecad4f50334dac702bd5a2d3ba01b91709f0e": map[string]interface{}{
			"payment-creation-info": "0x02acee76ebd53d00824410cf6adecad4f50334dac702bd5a2d3ba01b91709f0e00000000004c4b40000000005e4fb7ab00000000",
			"payment-fail-info":     "0x03",
			"payment-htlcs-bucket": map[string]interface{}{
				"0x0000000000000001": map[string]interface{}{
					"htlc-attempt-info": "0x2997a72e129fc9d638ef2fa4e233567d808d4f18a4f087637582427962eb3bf800005ce600000000004c4b402102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000004c4b4000000000000000000000000000",
					"htlc-fail-info":    "0x0000000000000000000000000000",
				},
			},
			"payment-sequence-key": "0x0000000000000001",
		},
		"0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840c": map[string]interface{}{
			"payment-creation-info": "0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840c00000000005b8d80000000005e4fb97f000000fc6c6e62637274363075317030796c7774367070357674346e377a6a676c39327766397773633939767630307366666e7561376a656d74376d6535373471336b3337346a387373787164717163717a70677370353835357075743937713863747374776b7735796b306a667278736e746e7a6878326a77786a636d3937346c636437327a3564757339717939717371653872336b3578733379367868667366366d6a6e706d717172306661797a677a63336a6b663571787a6c376866787a6666763578667a7679647564327275767974706571787072376868796830726a747574373033333274737774686661616e303773766b6667716b7174667275",
			"payment-htlcs-bucket": map[string]interface{}{
				"0x00000000000003e8": map[string]interface{}{
					"htlc-attempt-info": "0x8de663f9bb4b8d1ebdb496d22dc1cb657a346215607308549f41b01e2adf2ce900005ce600000000005b8d802102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000005b8d8000000000010000000000000008233d281e2cbe01f0b82dd6750967c9233426b98ae6549c696365f57f86f942a3795b8d800000000000000000",
					"htlc-settle-info":  "0x479593b7d3cbb45beb22d448451a2f3619b2095adfb38f4d92e9886e965343680000000000000000",
				},
			},
			"payment-sequence-key": "0x0000000000000002",
		},
		"0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840d": map[string]interface{}{
			"payment-creation-info": "0x62eb3f0a48f954e495d0c14ac63df04a67cefa59dafdbcd3d5046d1f5647840d00000000006acfc0000000005e4fb98d000000fc6c6e62637274373075317030796c7776327070357674346e377a6a676c39327766397773633939767630307366666e7561376a656d74376d6535373471336b3337346a387373787364717163717a706773703578707a307964663467336572727a656372376b6e7567307474667630327a7665727a72676b70737375376d6d6564617934687973397179397173717774656479336e666c323534787a36787a75763974746767757a647473356e617a7461616a6735667772686438396b336d70753971726d7a6c3779637a306e30666e6e763077753032726632706e64636c393761646c667636376a7a6e7063677477356434366771323571326e32",
			"payment-htlcs-bucket": map[string]interface{}{
				"0x00000000000003e9": map[string]interface{}{
					"htlc-attempt-info": "0x53ce0a4c1507cc5ea00ec88b76bd43a3978ac13605497030b821af6ce9c110f300005ce600000000006acfc02102ec12e83eafe27ce6d03bbe0c0de4b79fe2b9934615c8aa7693f73d2e41b089700000000121028c2dd128c7a6c1a0fceb3e3eb5ed55e0a0ae1a939eb786b097322d830d47db75005ca4000001000000005ce600000000006acfc000000000010000000000000008233044f235354472318b381fad3e21eb5a58f5099918868b0610e7b7bcb7a4adc96acfc00000000000000000",
				},
			},
			"payment-sequence-key": "0x0000000000000003",
		},
	}
)

// TestMigrateMpp asserts that the database is properly migrated to the mpp
// payment structure.
func TestMigrateMpp(t *testing.T) {
	var paymentsRootBucket = []byte("payments-root-bucket")

	migtest.ApplyMigration(
		t,
		func(tx *bbolt.Tx) error {
			return migtest.WriteDB(tx, paymentsRootBucket, pre)
		},
		func(tx *bbolt.Tx) error {
			return migtest.VerifyDB(tx, paymentsRootBucket, post)
		},
		MigrateMPP,
		false,
	)
}
