import { Component } from "react";

export default class ErrorBoundary extends Component {
  state = { error: null };
  static getDerivedStateFromError(e) { return { error: e }; }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding:"32px 14px", fontFamily:"monospace",
            fontSize:11, color:"var(--red, #e05252)", textAlign:"center" }}>
          {this.state.error.message}
        </div>
      );
    }
    return this.props.children;
  }
}