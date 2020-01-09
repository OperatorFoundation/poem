package poem

import "testing"

func TestConnect(t *testing.T) {
	_, connectError := Connect(ConnectOpts{Address:"test"})
	if connectError != nil {
		t.Fail()
		return
	}
}

func TestDBCreate(t *testing.T) {
	session, connectError := Connect(ConnectOpts{Address:"test"})
	if connectError != nil {
		t.Fail()
		return
	}

	_, createError := DBCreate("test").RunWrite(session)
	if createError != nil {
		t.Fail()
		return
	}
}

func TestDB_TableCreate(t *testing.T) {
	session, connectError := Connect(ConnectOpts{Address:"test"})
	if connectError != nil {
		t.Fail()
		return
	}

	_, createError := DBCreate("test").RunWrite(session)
	if createError != nil {
		t.Fail()
		return
	}

	_, tableCreateError := DB{"test"}.TableCreate("test").RunWrite(session)
	if tableCreateError != nil {
		t.Fail()
		return
	}
}

func TestTable_Insert(t *testing.T) {
	session, connectError := Connect(ConnectOpts{Address:"test"})
	if connectError != nil {
		t.Fail()
		return
	}

	_, createError := DBCreate("test").RunWrite(session)
	if createError != nil {
		t.Fail()
		return
	}

	_, tableCreateError := DB{"test"}.TableCreate("test").RunWrite(session)
	if tableCreateError != nil {
		t.Fail()
		return
	}

	_, insertError := DB{"test"}.Table("test").Insert("test").RunWrite(session)
	if insertError != nil {
		t.Fail()
		return
	}
}