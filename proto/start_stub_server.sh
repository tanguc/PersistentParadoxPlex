docker run -p 4770:4770 -p 4771:4771 -v C:\Users\stanguc\Projects\PersistentMarkingLB\proto:/proto -v C:\Users\stanguc\Projects\PersistentMarkingLB\proto\backend_stub.json:/stub/backend.json  tkpd/gripmock --stub=/stub /proto/backend.proto