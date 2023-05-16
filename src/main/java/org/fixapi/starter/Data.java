package org.fixapi.starter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.Application;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.field.Account;
import quickfix.field.EncryptMethod;
import quickfix.field.MDEntryPx;
import quickfix.field.MDEntryType;
import quickfix.field.MDReqID;
import quickfix.field.MDUpdateType;
import quickfix.field.MarketDepth;
import quickfix.field.NoMDEntries;
import quickfix.field.Password;
import quickfix.field.ResetSeqNumFlag;
import quickfix.field.SendingTime;
import quickfix.field.SubscriptionRequestType;
import quickfix.field.Symbol;
import quickfix.field.Username;
import quickfix.fix44.Heartbeat;
import quickfix.fix44.Logon;
import quickfix.fix44.Logout;
import quickfix.fix44.MarketDataIncrementalRefresh;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.MarketDataSnapshotFullRefresh;
import quickfix.fix44.MessageCracker;

public class Data extends MessageCracker implements Application {
	private static final Logger logger = LoggerFactory.getLogger(Data.class);

	private boolean resetReq;
	String mdReqID;

	private String userName;
	private String userPassword;
	private String accountId;

	private long requestID;
	private SessionID sessionID;

	List<String> symbolsList;

	public Data(SessionSettings settings, boolean resetReq) {
		this.resetReq = resetReq;

		try {
			userName = settings.getString("Username");
			userPassword = settings.getString("Password");
			accountId = settings.getString("AccountId");

			symbolsList = new ArrayList<String>();
			symbolsList.add("EUR/USD");
			symbolsList.add("EUR/GBP");
			symbolsList.add("GBP/USD");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	private synchronized long nextID() {
		requestID++;
		if (requestID > 0x7FFFFFF0) {
			requestID = 1;
		}
		return requestID;
	}

	public void fromAdmin(Message message, SessionID sessionID) {
		try {
			crack(message, sessionID);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void fromApp(Message message, SessionID sessionID) {
		try {
			crack(message, sessionID);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void toAdmin(Message message, SessionID sessionID) {
		try {
			if (message instanceof Logon) {
				logger.info("Data via toAdmin login begun for " + this.userName);

				message.setString(Username.FIELD, userName);
				message.setString(Password.FIELD, userPassword);
				if (resetReq) {
					message.setBoolean(ResetSeqNumFlag.FIELD, ResetSeqNumFlag.YES_RESET_SEQUENCE_NUMBERS);
				}
				message.setInt(EncryptMethod.FIELD, EncryptMethod.NONE_OTHER);
			} else if (message instanceof Logout) {
				logger.info("Data logged out via toAdmin " + this.userName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void toApp(Message message, SessionID sessionID) {
	}

	public void onCreate(SessionID sessionID) {
		this.sessionID = sessionID;
	}

	public void logout() {
		sendMarketDataRequestList(SubscriptionRequestType.DISABLE_PREVIOUS_SNAPSHOT_UPDATE_REQUEST);

		Logout mdr = new Logout();
		send(mdr);
	}

	public void onLogon(SessionID sessionID) {
		logger.warn("Data via onLogon login begun" + (userName == null ? "" : " for " + userName) );

		sendMarketDataRequestList(SubscriptionRequestType.SNAPSHOT_UPDATES);
	}

	public void onLogout(SessionID sessionID) {
		logger.warn("Data logged out via onLogout" + (userName == null ? "" : " for " + userName) );
	}

	private void send(Message message) {
		try {
			Session.sendToTarget(message, sessionID);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	private void sendMarketDataRequestList(char subscriptionRequestType) {
		try {
			MarketDataRequest mdr = new MarketDataRequest();
			mdr.set(new SubscriptionRequestType(subscriptionRequestType));
			mdr.set(new MarketDepth(1)); // Top of Book

			if (subscriptionRequestType == SubscriptionRequestType.SNAPSHOT_UPDATES) {
				mdr.set(new MDUpdateType(MDUpdateType.FULL_REFRESH));

				long nextId = nextID();
				mdr.set(new MDReqID(String.valueOf(nextId)));
			} else {
				if (mdReqID != null) {
					mdr.set(new MDReqID(mdReqID));
				} else {
					return;
				}
			}

			for (String symbolName : symbolsList) {
				MarketDataRequest.NoMDEntryTypes types = null;
				types = new MarketDataRequest.NoMDEntryTypes();
				types.set(new MDEntryType(MDEntryType.BID));
				mdr.addGroup(types);

				types = new MarketDataRequest.NoMDEntryTypes();
				types.set(new MDEntryType(MDEntryType.OFFER));
				mdr.addGroup(types);

				MarketDataRequest.NoRelatedSym symbol = new MarketDataRequest.NoRelatedSym();
				symbol.set(new quickfix.field.Symbol(symbolName));
				mdr.addGroup(symbol);
			}

			mdr.setString(Account.FIELD, accountId);

			send(mdr);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	// Snapshot
	public void onMessage(MarketDataSnapshotFullRefresh snapshot, SessionID sessionID) {
		try {
			SendingTime sendingTime = new SendingTime();
			((Message) snapshot).getHeader().getField(sendingTime);
			LocalDateTime localDateTime = sendingTime.getValue();
			long time = localDateTime.toEpochSecond(ZoneOffset.UTC);

			String symbolName = null;

			quickfix.field.Symbol symbol = new quickfix.field.Symbol();
			snapshot.get(symbol);

			symbolName = symbol.getValue();

			NoMDEntries noMDEntries = new NoMDEntries();
			snapshot.get(noMDEntries);

			MarketDataSnapshotFullRefresh.NoMDEntries types = new MarketDataSnapshotFullRefresh.NoMDEntries();
			MDEntryType mdEntryType = new MDEntryType();
			MDEntryPx mdEntryPx = new MDEntryPx();

			snapshot.getGroup(1, types);
			types.get(mdEntryType);

			double bid = 0.0;
			if (mdEntryType.getValue() == MDEntryType.BID) {
				types.get(mdEntryPx);
				bid = mdEntryPx.getValue();
			}

			snapshot.getGroup(2, types);
			types.get(mdEntryType);

			double ask = 0.0;
			if (mdEntryType.getValue() == MDEntryType.OFFER) {
				types.get(mdEntryPx);
				ask = mdEntryPx.getValue();
			}

			System.out.println(time + " " + symbolName + ": Ask " + ask + ", Bid " + bid);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	// Update
	public void onMessage(MarketDataIncrementalRefresh snapshot, SessionID sessionID) {
		try {
			SendingTime sendingTime = new SendingTime();
			((Message) snapshot).getHeader().getField(sendingTime);
			LocalDateTime localDateTime = sendingTime.getValue();
			long time = localDateTime.toEpochSecond(ZoneOffset.UTC);

			NoMDEntries noMDEntries = new NoMDEntries();
			snapshot.get(noMDEntries);

			for (int i = 1; i <= noMDEntries.getValue(); i++) {
				MarketDataSnapshotFullRefresh.NoMDEntries types = new MarketDataSnapshotFullRefresh.NoMDEntries();
				snapshot.getGroup(i, types);

				String symbolName = types.getString(Symbol.FIELD);

				MDEntryType mdEntryType = new MDEntryType();
				MDEntryPx mdEntryPx = new MDEntryPx();

				types.get(mdEntryType);

				double bid = 0.0;
				if (mdEntryType.getValue() == MDEntryType.BID) {
					types.get(mdEntryPx);
					bid = mdEntryPx.getValue();
				}

				double ask = 0.0;
				if (mdEntryType.getValue() == MDEntryType.OFFER) {
					types.get(mdEntryPx);
					ask = mdEntryPx.getValue();
				}

				System.out.println(time + " " + symbolName + ": Ask " + ask + ", Bid " + bid);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void onMessage(Heartbeat heartbeat, SessionID sessionID) {
	}
}