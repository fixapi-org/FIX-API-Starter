package org.fixapi.starter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import quickfix.Application;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.SessionSettings;
import quickfix.field.Account;
import quickfix.field.AvgPx;
import quickfix.field.ClOrdID;
import quickfix.field.CumQty;
import quickfix.field.EncryptMethod;
import quickfix.field.ExecID;
import quickfix.field.ExecType;
import quickfix.field.LeavesQty;
import quickfix.field.OrdStatus;
import quickfix.field.OrdType;
import quickfix.field.OrderID;
import quickfix.field.OrderQty;
import quickfix.field.Password;
import quickfix.field.ResetSeqNumFlag;
import quickfix.field.SendingTime;
import quickfix.field.Side;
import quickfix.field.Symbol;
import quickfix.field.Text;
import quickfix.field.TimeInForce;
import quickfix.field.TransactTime;
import quickfix.field.Username;
import quickfix.fix44.ExecutionReport;
import quickfix.fix44.Heartbeat;
import quickfix.fix44.Logon;
import quickfix.fix44.Logout;
import quickfix.fix44.MessageCracker;
import quickfix.fix44.NewOrderSingle;

public class Order extends MessageCracker implements Application {
	private static final Logger logger = LoggerFactory.getLogger(Order.class);

	private boolean resetReq;

	private String userName;
	private String userPassword;
	private String accountId;

	private SessionID sessionID;

	public Order(SessionSettings settings, boolean resetReq) {
		this.resetReq = resetReq;

		try {
			userName = settings.getString("Username");
			userPassword = settings.getString("Password");
			accountId = settings.getString("AccountId");

		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
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

	// Order
	public void toAdmin(Message message, SessionID sessionID) {
		try {
			if (message instanceof Logon) {
				logger.info("Order via toAdmin login begun for " + this.userName);

				message.setString(Username.FIELD, userName);
				message.setString(Password.FIELD, userPassword);
				if (resetReq) {
					message.setBoolean(ResetSeqNumFlag.FIELD, ResetSeqNumFlag.YES_RESET_SEQUENCE_NUMBERS);
				}
				message.setInt(EncryptMethod.FIELD, EncryptMethod.NONE_OTHER);
			} else if (message instanceof Logout) {
				logger.info("Order logged out via toAdmin " + this.userName);
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
		Logout mdr = new Logout();
		send(mdr);
	}

	public void onLogon(SessionID sessionID) {
		logger.warn("Order via onLogon login begun" + (userName == null ? "" : " for " + userName) );
	}

	public void onLogout(SessionID sessionID) {
		logger.warn("Order logged out via onLogout" + (userName == null ? "" : " for " + userName) );
	}

	private void send(Message message) {
		try {
			Session.sendToTarget(message, sessionID);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void sendMarketOrder(String clOrderId, String symbolName, double lots, String orderType, double price) {
		try {
			NewOrderSingle order = new NewOrderSingle();

			order.set(new ClOrdID(clOrderId));
			order.set(new OrdType(OrdType.MARKET));
			order.set(new Symbol(symbolName));

			if ("BUY".equals(orderType)) {
				order.set(new Side(Side.BUY));
			} else {
				order.set(new Side(Side.SELL));
			}

			double orderQty = lots * 100000;
			order.set(new OrderQty(orderQty));

			order.set(new TimeInForce(TimeInForce.FILL_OR_KILL));

			order.set(new TransactTime());

			order.setString(Account.FIELD, accountId);

			send(order);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void onMessage(ExecutionReport executionReport, SessionID sessionID) {
		try {
			SendingTime sendingTime = new SendingTime();
			((Message) executionReport).getHeader().getField(sendingTime);
			LocalDateTime localDateTime = sendingTime.getValue();
			long time = localDateTime.toEpochSecond(ZoneOffset.UTC);

			ClOrdID clOrdID = new ClOrdID();
			executionReport.get(clOrdID);
			String clOrderId = clOrdID.getValue();

			OrderID orderID = null;
			String orderId = "";
			if (executionReport.isSetOrderID()) {
				orderID = new OrderID();
				executionReport.get(orderID);
				orderId = orderID.getValue();
			}

			ExecID execID = null;
			String execId = "";
			if (executionReport.isSetExecID()) {
				execID = new ExecID();
				executionReport.get(execID);
				execId = execID.getValue();
			}

			ExecType execType = new ExecType();
			executionReport.get(execType);
			String executionType = null;
			if (execType.getValue() == ExecType.PENDING_NEW) {
				executionType = "PENDING_NEW";
			} else if (execType.getValue() == ExecType.NEW) {
				executionType = "NEW";
			} else if (execType.getValue() == ExecType.CANCELED) {
				executionType = "CANCELED";
			} else if (execType.getValue() == ExecType.PENDING_CANCEL) {
				executionType = "PENDING_CANCEL";
			} else if (execType.getValue() == ExecType.REJECTED) {
				executionType = "REJECTED";
			} else if (execType.getValue() == ExecType.TRADE) {
				executionType = "TRADE";
			} else if (execType.getValue() == ExecType.FILL) {
				executionType = "FILLED";
			} else if (execType.getValue() == ExecType.ORDER_STATUS) {
				executionType = "ORDER_STATUS";
			} else {
				executionType = "NOT_SUPPORTED";
			}

			OrdStatus ordStatus = new OrdStatus();
			executionReport.get(ordStatus);
			String orderStatus = null;
			if (ordStatus.getValue() == OrdStatus.NEW) {
				orderStatus = "NEW";
			} else if (ordStatus.getValue() == OrdStatus.PARTIALLY_FILLED) {
				orderStatus = "PARTIALLY_FILLED";
			} else if (ordStatus.getValue() == OrdStatus.FILLED) {
				orderStatus = "FILLED";
			} else if (ordStatus.getValue() == OrdStatus.CANCELED) {
				orderStatus = "CANCELED";
			} else if (ordStatus.getValue() == OrdStatus.PENDING_CANCEL) {
				orderStatus = "PENDING_CANCEL";
			} else if (ordStatus.getValue() == OrdStatus.REJECTED) {
				orderStatus = "REJECTED";
			} else if (ordStatus.getValue() == OrdStatus.PENDING_NEW) {
				orderStatus = "PENDING_NEW";
			} else {
				orderStatus = "NOT_SUPPORTED";
			}

			if (executionType.equals("REJECTED")
				|| orderStatus.equals("REJECTED")) {

				System.out.println(time + " clOrderId: " + clOrderId + ", orderId: " + orderId + ", execId: " + execId + ", rejected by the LP");
			} else {				
				TimeInForce timeInForce = new TimeInForce();
				String tmInForce = null;

				if (executionReport.isSetTimeInForce()) {
					executionReport.get(timeInForce);

					if (timeInForce.getValue() == TimeInForce.IMMEDIATE_OR_CANCEL) {
						tmInForce = "IMMEDIATE_OR_CANCEL";
					} else if (timeInForce.getValue() == TimeInForce.FILL_OR_KILL) {
						tmInForce = "FILL_OR_KILL";
					} else {
						tmInForce = "";
					}
				} else {
					tmInForce = "NOT_SUPPORTED";
				}
	
				quickfix.field.Symbol symbol = new quickfix.field.Symbol();
				executionReport.get(symbol);
				String symbolName = symbol.getValue();

				Side side = new Side();
				executionReport.get(side);
				String orderSide = null;
				if (side.getValue() == Side.BUY) {
					orderSide = "BUY";
				} else if (side.getValue() == Side.SELL) {
					orderSide = "SELL";
				} else {
					orderSide = "NOT_SUPPORTED";
				}
	
				AvgPx avgPx = null;
				double avgPrice = 0.0;
				if (executionReport.isSetAvgPx()) {
					avgPx = new AvgPx();
					executionReport.get(avgPx);
					avgPrice = avgPx.getValue();
				}

				OrderQty orderQty = null;
				double ordQty = 0.0;
				if (executionReport.isSetOrderQty()) {
					orderQty = new OrderQty();
					executionReport.get(orderQty);
					ordQty = orderQty.getValue();

					ordQty *= 100000;
				}
	
				LeavesQty leavesQty = new LeavesQty();
				executionReport.get(leavesQty);
				double leavesQuantity = leavesQty.getValue();
				leavesQuantity *= 100000;
	
				CumQty cumQty = new CumQty();
				executionReport.get(cumQty);
				double cumQuantity = cumQty.getValue();
				cumQuantity *= 100000;
	
				Text text = new Text();
				String reason = "";
				if (executionReport.isSet(text)) {
					executionReport.getField(text);
					reason = text.getValue();
				}
	
				TransactTime transactTime = null;
				LocalDateTime transactionTime = null;
				long transactTm = (long) Math.floor(System.currentTimeMillis() / 1000.0);
				if (executionReport.isSetTransactTime()) {
					transactTime = new TransactTime();
					executionReport.get(transactTime);
					transactionTime = transactTime.getValue();
					transactTm = transactionTime.toEpochSecond(ZoneOffset.UTC);
				}

				System.out.println("A trade position is opened!");
				System.out.println("SendingTime: " + time);
				System.out.println("ClOrdID: " + clOrderId);
				System.out.println("OrderID: " + orderId);
				System.out.println("ExecID: " + execId);
				System.out.println("ExecType: " + executionType);
				System.out.println("OrdStatus: " + orderStatus);
				System.out.println("TimeInForce: " + tmInForce);
				System.out.println("Symbol: " + symbolName);
				System.out.println("Side: " + orderSide);
				System.out.println("AvgPx: " + avgPrice);
				System.out.println("OrderQty: " + ordQty);
				System.out.println("LeavesQty: " + leavesQuantity);
				System.out.println("CumQty: " + cumQuantity);
				System.out.println("Text: " + reason);
				System.out.println("TransactTime: " + transactTm);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}
	}

	public void onMessage(Heartbeat heartbeat, SessionID sessionID) {
	}
}